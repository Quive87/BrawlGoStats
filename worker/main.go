package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
)

// ─── Config ──────────────────────────────────────────────────────────────────

const (
	dbPath        = "/var/www/BrawlGoStats/brawl_data.sqlite"
	baseURL       = "https://api.brawlstars.com/v1"
	lockFile      = "/tmp/brawl-worker.lock"
	batchLoadSize = 2000 // tags fetched per DB poll
)

// ─── Rate Limiter (Token Bucket) ─────────────────────────────────────────────

type TokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	maxRate  float64 // tokens per second
	lastFill time.Time
}

func NewTokenBucket(rps float64) *TokenBucket {
	return &TokenBucket{tokens: rps, maxRate: rps, lastFill: time.Now()}
}

func (tb *TokenBucket) Wait() {
	for {
		tb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tb.lastFill).Seconds()
		tb.tokens = min(tb.maxRate, tb.tokens+elapsed*tb.maxRate)
		tb.lastFill = now
		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return
		}
		tb.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

var (
	totalRequests  uint64
	enriched       uint64
	discovered     uint64
	rateLimitsHit  uint64
	errors429      uint64
	errorsOther    uint64
)

// ─── DB Write Payloads ───────────────────────────────────────────────────────

type WriteOp struct {
	opType string
	data   interface{}
}

// ─── API Types ───────────────────────────────────────────────────────────────

type ProfileResponse struct {
	Tag                                string  `json:"tag"`
	Name                               string  `json:"name"`
	Trophies                           int     `json:"trophies"`
	HighestTrophies                    int     `json:"highestTrophies"`
	TotalPrestigeLevel                 int     `json:"totalPrestigeLevel"`
	ExpLevel                           int     `json:"expLevel"`
	ExpPoints                          int     `json:"expPoints"`
	IsQualifiedFromChampionshipChallenge bool   `json:"isQualifiedFromChampionshipChallenge"`
	Victories3v3                       int     `json:"3vs3Victories"`
	SoloVictories                      int     `json:"soloVictories"`
	DuoVictories                       int     `json:"duoVictories"`
	Icon                               struct {
		ID int `json:"id"`
	} `json:"icon"`
	Club struct {
		Tag  string `json:"tag"`
		Name string `json:"name"`
	} `json:"club"`
	Brawlers []json.RawMessage `json:"brawlers"`
}

type BrawlerItem struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Trophies    int    `json:"trophies"`
	Skin        *struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"skin"`
	Gadgets     []struct{ ID int; Name string } `json:"gadgets"`
	StarPowers  []struct{ ID int; Name string } `json:"starPowers"`
	Gears       []struct{ ID int; Name string } `json:"gears"`
	HyperCharges []struct{ ID int; Name string } `json:"hyperCharges"`
}

type BattleEntry struct {
	BattleTime string `json:"battleTime"`
	Event      struct {
		ID   int    `json:"id"`
		Mode string `json:"mode"`
		Map  string `json:"map"`
	} `json:"event"`
	Battle struct {
		Mode         string `json:"mode"`
		Type         string `json:"type"`
		Result       string `json:"result"`
		Duration     int    `json:"duration"`
		Rank         int    `json:"rank"`
		TrophyChange int    `json:"trophyChange"`
		StarPlayer   *struct {
			Tag string `json:"tag"`
		} `json:"starPlayer"`
		Teams   [][]PlayerInfo `json:"teams"`
		Players []PlayerInfo   `json:"players"`
	} `json:"battle"`
}

type PlayerInfo struct {
	Tag     string `json:"tag"`
	Name    string `json:"name"`
	Brawler *struct {
		ID       int    `json:"id"`
		Name     string `json:"name"`
		Power    int    `json:"power"`
		Trophies int    `json:"trophies"`
		Skin     *struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"skin"`
	} `json:"brawler"`
	Brawlers []struct {
		ID           int    `json:"id"`
		Name         string `json:"name"`
		Power        int    `json:"power"`
		Trophies     int    `json:"trophies"`
		TrophyChange int    `json:"trophyChange"`
	} `json:"brawlers"`
	TrophyChange int `json:"-"` // set externally
	IsWinner     int `json:"-"`
	Result       string `json:"-"`
	TeamID       int    `json:"-"`
}

type ClubMembersResponse struct {
	Items []struct {
		Tag string `json:"tag"`
	} `json:"items"`
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	// Load .env from project root
	_ = godotenv.Load("/var/www/BrawlGoStats/.env")
	if _, err := os.Stat("/var/www/BrawlGoStats/.env"); err != nil {
		_ = godotenv.Load(".env")
	}

	apiToken := os.Getenv("SUPERCELL_API_TOKEN")
	if apiToken == "" {
		fmt.Fprintln(os.Stderr, "FATAL: SUPERCELL_API_TOKEN not set")
		os.Exit(1)
	}

	// Worker count from env (default 120)
	numWorkers := 120
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		fmt.Sscanf(v, "%d", &numWorkers)
	}

	// Lock file — prevent double-start
	if _, err := os.Stat(lockFile); err == nil {
		fmt.Fprintln(os.Stderr, "FATAL: another instance is already running (lock file exists). Remove", lockFile, "to override.")
		os.Exit(1)
	}
	lf, err := os.Create(lockFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL: cannot create lock file:", err)
		os.Exit(1)
	}
	lf.Close()
	defer os.Remove(lockFile)

	// DB connect (WAL, tuned PRAGMAs)
	db, err := sql.Open("sqlite", dbPath+
		"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=-65536&_temp_store=MEMORY")
	if err != nil {
		fmt.Fprintln(os.Stderr, "FATAL: DB connect:", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)  // SQLite: serialise writers
	db.SetMaxIdleConns(1)

	// HTTP client — reused by all workers
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:          numWorkers + 20,
			MaxIdleConnsPerHost:   numWorkers + 20,
			IdleConnTimeout:       60 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
		},
		Timeout: 20 * time.Second,
	}

	// Rate limiter: 70 RPS (safe side of 75-80 limit)
	rateLimiter := NewTokenBucket(70)

	// Channels
	enrichQueue := make(chan string, 20000)  // priority: profile enrichment
	discovQueue := make(chan string, 10000)  // secondary: battlelog discovery
	dbChan      := make(chan WriteOp, 50000) // DB write queue

	var wg sync.WaitGroup
	done := make(chan struct{})

	// ── Seed empty DB from leaderboard ──
	go func() {
		if isEmpty(db) {
			fmt.Println("[SEED] DB is empty — seeding from global/country leaderboards...")
			seedFromLeaderboard(httpClient, apiToken, enrichQueue)
			fmt.Println("[SEED] Done.")
		}
	}()

	// ── DB Writer goroutine ──
	wg.Add(1)
	go dbWriter(db, dbChan, &wg)

	// ── Workers ──
	// 80% enrichment, 20% discovery
	enrichWorkers := numWorkers * 4 / 5
	discovWorkers := numWorkers - enrichWorkers

	for i := 0; i < enrichWorkers; i++ {
		wg.Add(1)
		go enrichWorker(enrichQueue, dbChan, httpClient, apiToken, rateLimiter, done, &wg)
	}
	for i := 0; i < discovWorkers; i++ {
		wg.Add(1)
		go discoveryWorker(discovQueue, enrichQueue, dbChan, httpClient, apiToken, rateLimiter, done, &wg)
	}

	// ── Queue filler goroutine ──
	go queueFiller(db, enrichQueue, discovQueue, done)

	// ── Metrics reporter ──
	go reporter()

	fmt.Printf("[BRAWL-WORKER] Started: %d enrichment workers + %d discovery workers (70 RPS limit)\n",
		enrichWorkers, discovWorkers)

	// ── Graceful shutdown ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("[BRAWL-WORKER] Shutting down...")
	close(done)
	wg.Wait()
	close(dbChan)
	fmt.Println("[BRAWL-WORKER] Stopped.")
}

// ─── isEmpty ────────────────────────────────────────────────────────────────

func isEmpty(db *sql.DB) bool {
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM players").Scan(&count)
	return count == 0
}

// ─── seedFromLeaderboard ────────────────────────────────────────────────────

func seedFromLeaderboard(client *http.Client, token string, out chan<- string) {
	countryCodes := []string{
		"global", "af", "al", "dz", "ar", "am", "au", "at", "az", "bs", "bh", "bd", "be", "bz",
		"bj", "bt", "bo", "ba", "bw", "br", "bn", "bg", "bf", "bi", "kh", "cm", "ca", "cv", "cf",
		"td", "cl", "cn", "co", "cg", "cr", "ci", "hr", "cu", "cy", "cz", "dk", "do", "ec", "eg",
		"sv", "er", "et", "fj", "fi", "fr", "ga", "gm", "ge", "de", "gh", "gr", "gt", "gn", "gy",
		"ht", "hn", "hk", "hu", "is", "in", "id", "ir", "iq", "ie", "il", "it", "jm", "jp", "jo",
		"kz", "ke", "kw", "kg", "la", "lv", "lb", "ls", "lr", "ly", "li", "lt", "lu", "mg", "mw",
		"my", "mv", "ml", "mt", "mr", "mu", "mx", "md", "mc", "mn", "me", "ma", "mz", "mm", "na",
		"np", "nl", "nz", "ni", "ne", "ng", "no", "om", "pk", "pa", "pg", "py", "pe", "ph", "pl",
		"pt", "pr", "qa", "ro", "ru", "rw", "sa", "sn", "rs", "sl", "sg", "sk", "si", "so", "za",
		"kr", "es", "lk", "sd", "se", "ch", "tw", "tj", "tz", "th", "tg", "to", "tt", "tn", "tr",
		"tm", "ug", "ua", "ae", "gb", "us", "uy", "uz", "ve", "vn", "ye", "zm", "zw",
	}

	seen := make(map[string]bool)
	for _, code := range countryCodes {
		url := fmt.Sprintf("%s/rankings/%s/players", baseURL, code)
		body, status := doGET(client, url, token)
		if status != 200 || body == nil {
			continue
		}
		var resp struct {
			Items []struct{ Tag string `json:"tag"` } `json:"items"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}
		for _, p := range resp.Items {
			tag := cleanTag(p.Tag)
			if tag != "" && !seen[tag] {
				seen[tag] = true
				select {
				case out <- tag:
				default:
				}
			}
		}
	}
	fmt.Printf("[SEED] Queued %d unique tags from leaderboards\n", len(seen))
}

// ─── Queue Filler ────────────────────────────────────────────────────────────

func queueFiller(db *sql.DB, enrichQ, discovQ chan string, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}

		// Fill enrichment queue (priority)
		if len(enrichQ) < 5000 {
			rows, _ := db.Query(`
				SELECT tag FROM players
				WHERE profile_updated_at IS NULL
				LIMIT ?`, batchLoadSize)
			if rows != nil {
				count := 0
				for rows.Next() {
					var tag string
					if rows.Scan(&tag) == nil {
						select {
						case enrichQ <- tag:
							count++
						default:
						}
					}
				}
				rows.Close()
				if count > 0 {
					fmt.Printf("[FILLER] Fed %d tags → enrichment queue\n", count)
				}
			}
		}

		// Fill discovery queue (only when enrichment is nearly empty)
		if len(enrichQ) < 500 && len(discovQ) < 2000 {
			rows, _ := db.Query(`
				SELECT tag FROM players
				WHERE profile_updated_at IS NOT NULL
				  AND (last_battlelog_scan IS NULL OR last_battlelog_scan < datetime('now', '-1 day'))
				LIMIT ?`, batchLoadSize/2)
			if rows != nil {
				for rows.Next() {
					var tag string
					if rows.Scan(&tag) == nil {
						select {
						case discovQ <- tag:
						default:
						}
					}
				}
				rows.Close()
			}
		}

		time.Sleep(3 * time.Second)
	}
}

// ─── Enrichment Worker ───────────────────────────────────────────────────────

func enrichWorker(
	queue chan string,
	dbChan chan<- WriteOp,
	client *http.Client,
	token string,
	rl *TokenBucket,
	done <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	seenClubs := make(map[string]bool)

	for {
		select {
		case <-done:
			return
		case tag, ok := <-queue:
			if !ok {
				return
			}
			rl.Wait()
			body, status := doGET(client, fmt.Sprintf("%s/players/%%23%s", baseURL, tag), token)
			atomic.AddUint64(&totalRequests, 1)

			if status == 429 {
				atomic.AddUint64(&rateLimitsHit, 1)
				atomic.AddUint64(&errors429, 1)
				time.Sleep(2 * time.Second)
				// Re-queue
				select {
				case queue <- tag:
				default:
				}
				continue
			}
			if status != 200 || body == nil {
				if status == 404 {
					dbChan <- WriteOp{"status_404", tag}
				} else {
					atomic.AddUint64(&errorsOther, 1)
				}
				continue
			}

			var p ProfileResponse
			if err := json.Unmarshal(body, &p); err != nil {
				atomic.AddUint64(&errorsOther, 1)
				continue
			}

			// Compress brawlers blob
			brawlersJSON, _ := json.Marshal(p.Brawlers)

			clubTag := cleanTag(p.Club.Tag)
			dbChan <- WriteOp{"profile", profileData{
				Tag:            tag,
				Name:           p.Name,
				Trophies:       p.Trophies,
				HighestTrophies: p.HighestTrophies,
				TotalPrestige:  p.TotalPrestigeLevel,
				ExpLevel:       p.ExpLevel,
				ExpPoints:      p.ExpPoints,
				IsQualified:    boolToInt(p.IsQualifiedFromChampionshipChallenge),
				Victories3v3:   p.Victories3v3,
				VictoriesSolo:  p.SoloVictories,
				VictoriesDuo:   p.DuoVictories,
				ClubTag:        clubTag,
				ClubName:       p.Club.Name,
				IconID:         p.Icon.ID,
				BrawlersData:   brawlersJSON,
			}}
			atomic.AddUint64(&enriched, 1)

			// Write per-brawler data
			for _, raw := range p.Brawlers {
				var b BrawlerItem
				if err := json.Unmarshal(raw, &b); err != nil {
					continue
				}
				skinID, skinName := 0, ""
				if b.Skin != nil {
					skinID = b.Skin.ID
					skinName = b.Skin.Name
				}
				dbChan <- WriteOp{"brawler", brawlerData{PlayerTag: tag, BrawlerID: b.ID, BrawlerName: b.Name, Trophies: b.Trophies, SkinID: skinID, SkinName: skinName}}
				dbChan <- WriteOp{"brawler_stat", brawlerStatData{b.ID, b.Name}}
				for _, g := range b.Gadgets {
					dbChan <- WriteOp{"build", buildData{b.ID, g.ID, "gadget", g.Name}}
				}
				for _, sp := range b.StarPowers {
					dbChan <- WriteOp{"build", buildData{b.ID, sp.ID, "starpower", sp.Name}}
				}
				for _, gr := range b.Gears {
					dbChan <- WriteOp{"build", buildData{b.ID, gr.ID, "gear", gr.Name}}
				}
				for _, hc := range b.HyperCharges {
					dbChan <- WriteOp{"build", buildData{b.ID, hc.ID, "hypercharge", hc.Name}}
				}
			}

			// Club snowball: discover members
			if clubTag != "" && !seenClubs[clubTag] {
				seenClubs[clubTag] = true
				if len(seenClubs) > 30000 {
					seenClubs = make(map[string]bool)
				}
				rl.Wait()
				cBody, cStatus := doGET(client, fmt.Sprintf("%s/clubs/%%23%s/members", baseURL, clubTag), token)
				atomic.AddUint64(&totalRequests, 1)
				if cStatus == 200 && cBody != nil {
					var cr ClubMembersResponse
					if json.Unmarshal(cBody, &cr) == nil {
						for _, m := range cr.Items {
							mt := cleanTag(m.Tag)
							if mt != "" {
								dbChan <- WriteOp{"new_tag", mt}
							}
						}
					}
				}
			}
		}
	}
}

// ─── Discovery Worker ────────────────────────────────────────────────────────

func discoveryWorker(
	queue chan string,
	enrichQ chan string,
	dbChan chan<- WriteOp,
	client *http.Client,
	token string,
	rl *TokenBucket,
	done <-chan struct{},
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {
		case <-done:
			return
		case tag, ok := <-queue:
			if !ok {
				return
			}
			rl.Wait()
			body, status := doGET(client, fmt.Sprintf("%s/players/%%23%s/battlelog", baseURL, tag), token)
			atomic.AddUint64(&totalRequests, 1)

			if status == 429 {
				atomic.AddUint64(&rateLimitsHit, 1)
				atomic.AddUint64(&errors429, 1)
				time.Sleep(2 * time.Second)
				select {
				case queue <- tag:
				default:
				}
				continue
			}
			if status != 200 || body == nil {
				atomic.AddUint64(&errorsOther, 1)
				continue
			}

			var resp struct {
				Items []BattleEntry `json:"items"`
			}
			if json.Unmarshal(body, &resp) != nil {
				atomic.AddUint64(&errorsOther, 1)
				continue
			}

			newTags := processBattlelog(resp.Items, dbChan)
			for _, t := range newTags {
				dbChan <- WriteOp{"new_tag", t}
			}
			dbChan <- WriteOp{"scan_done", tag}
			atomic.AddUint64(&discovered, 1)
		}
	}
}

// ─── processBattlelog ────────────────────────────────────────────────────────

func processBattlelog(items []BattleEntry, dbChan chan<- WriteOp) []string {
	var newTags []string
	seenTags := make(map[string]bool)

	for _, item := range items {
		if item.Battle.Type == "friendly" {
			continue
		}
		mode := item.Event.Mode
		if mode == "" {
			mode = item.Battle.Mode
		}
		mapName := item.Event.Map
		mapID := item.Event.ID
		duration := item.Battle.Duration
		battleTrophyChange := item.Battle.TrophyChange

		starTag := ""
		if item.Battle.StarPlayer != nil {
			starTag = cleanTag(item.Battle.StarPlayer.Tag)
		}

		// Determine winner team index
		winnerTeam := -1
		switch item.Battle.Result {
		case "victory":
			winnerTeam = 0
		case "defeat":
			winnerTeam = 1
		}

		var players []PlayerInfo
		// Team modes
		for i, team := range item.Battle.Teams {
			for _, p := range team {
				pc := p
				pc.TeamID = i
				pc.TrophyChange = battleTrophyChange
				if i == winnerTeam || (item.Battle.Mode == "duoShowdown" && item.Battle.Rank <= 2) {
					pc.IsWinner = 1
					pc.Result = "victory"
				} else {
					pc.IsWinner = 0
					pc.Result = "defeat"
				}
				players = append(players, pc)
			}
		}
		// Solo modes
		for _, p := range item.Battle.Players {
			pc := p
			pc.TeamID = 0
			pc.TrophyChange = battleTrophyChange
			if (item.Battle.Mode == "soloShowdown" && item.Battle.Rank <= 4) ||
				item.Battle.Result == "victory" {
				pc.IsWinner = 1
				pc.Result = "victory"
			} else {
				pc.IsWinner = 0
				pc.Result = "defeat"
			}
			players = append(players, pc)
		}

		if len(players) == 0 {
			continue
		}

		// Deterministic match ID
		var allTags []string
		for _, p := range players {
			t := cleanTag(p.Tag)
			if t != "" {
				allTags = append(allTags, t)
			}
		}
		sort.Strings(allTags)
		hashInput := fmt.Sprintf("%s|%d|%s|%s", item.BattleTime, mapID, strings.Join(allTags, ","), mapName)
		matchID := fmt.Sprintf("%s-%x", item.BattleTime, sha256.Sum256([]byte(hashInput)))[:40]

		dbChan <- WriteOp{"match", matchData{
			MatchID:       matchID,
			BattleTime:    item.BattleTime,
			Mode:          mode,
			Type:          item.Battle.Type,
			Map:           mapName,
			MapID:         mapID,
			Duration:      duration,
			StarPlayerTag: starTag,
			EventID:       mapID,
			ModeID:        0,
		}}

		for _, p := range players {
			ptag := cleanTag(p.Tag)
			if ptag == "" {
				continue
			}
			if !seenTags[ptag] {
				seenTags[ptag] = true
				newTags = append(newTags, ptag)
			}

			if p.Brawler != nil {
				skinID, skinName := 0, ""
				if p.Brawler.Skin != nil {
					skinID = p.Brawler.Skin.ID
					skinName = p.Brawler.Skin.Name
				}
				dbChan <- WriteOp{"match_player", matchPlayerData{
					MatchID:        matchID,
					PlayerTag:      ptag,
					BrawlerName:    p.Brawler.Name,
					BrawlerID:      p.Brawler.ID,
					BrawlerPower:   p.Brawler.Power,
					BrawlerTrophies: p.Brawler.Trophies,
					SkinName:       skinName,
					SkinID:         skinID,
					IsWinner:       p.IsWinner,
					TeamID:         p.TeamID,
					TrophyChange:   p.TrophyChange,
					Result:         p.Result,
					Mode:           mode,
					Map:            mapName,
					MatchType:      item.Battle.Type,
				}}
			}
			// Duels
			for _, b := range p.Brawlers {
				dbChan <- WriteOp{"match_player", matchPlayerData{
					MatchID:        matchID,
					PlayerTag:      ptag,
					BrawlerName:    b.Name,
					BrawlerID:      b.ID,
					BrawlerPower:   b.Power,
					BrawlerTrophies: b.Trophies,
					IsWinner:       p.IsWinner,
					TeamID:         p.TeamID,
					TrophyChange:   b.TrophyChange,
					Result:         p.Result,
					Mode:           mode,
					Map:            mapName,
					MatchType:      item.Battle.Type,
				}}
			}
		}
	}
	return newTags
}

// ─── DB Writer ───────────────────────────────────────────────────────────────

type profileData struct {
	Tag, Name, ClubTag, ClubName string
	Trophies, HighestTrophies, TotalPrestige, ExpLevel, ExpPoints int
	IsQualified, Victories3v3, VictoriesSolo, VictoriesDuo       int
	IconID                                                         int
	BrawlersData                                                   []byte
}
type brawlerData     struct{ PlayerTag, BrawlerName string; BrawlerID, Trophies, SkinID int; SkinName string }
type brawlerStatData struct{ BrawlerID int; BrawlerName string }
type buildData       struct{ BrawlerID, ItemID int; ItemType, ItemName string }
type matchData       struct {
	MatchID, BattleTime, Mode, Type, Map, StarPlayerTag string
	MapID, Duration, EventID, ModeID                    int
}
type matchPlayerData struct {
	MatchID, PlayerTag, BrawlerName, SkinName  string
	BrawlerID, BrawlerPower, BrawlerTrophies   int
	SkinID, IsWinner, TeamID, TrophyChange     int
	Result, Mode, Map, MatchType               string
}

func dbWriter(db *sql.DB, in <-chan WriteOp, wg *sync.WaitGroup) {
	defer wg.Done()

	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()

	var batch []WriteOp
	flush := func() {
		if len(batch) == 0 {
			return
		}
		flushBatch(db, batch)
		batch = batch[:0]
	}

	for {
		select {
		case op, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, op)
			if len(batch) >= 1000 {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func flushBatch(db *sql.DB, batch []WriteOp) {
	tx, err := db.Begin()
	if err != nil {
		fmt.Println("[DB] Begin error:", err)
		return
	}

	stmts := map[string]*sql.Stmt{}
	prepare := func(name, q string) *sql.Stmt {
		if s, ok := stmts[name]; ok {
			return s
		}
		s, e := tx.Prepare(q)
		if e != nil {
			fmt.Println("[DB] Prepare error:", name, e)
			return nil
		}
		stmts[name] = s
		return s
	}

	for _, op := range batch {
		switch op.opType {
		case "profile":
			d := op.data.(profileData)
			s := prepare("profile", `
				UPDATE players SET
					profile_updated_at=CURRENT_TIMESTAMP, is_processed=1,
					name=?, icon_id=?, trophies=?, highest_trophies=?, total_prestige_level=?,
					exp_level=?, exp_points=?, is_qualified_from_championship_challenge=?,
					victories_3v3=?, victories_solo=?, victories_duo=?,
					club_tag=?, club_name=?, brawlers_data=?
				WHERE tag=?`)
			if s != nil {
				s.Exec(d.Name, d.IconID, d.Trophies, d.HighestTrophies, d.TotalPrestige,
					d.ExpLevel, d.ExpPoints, d.IsQualified,
					d.Victories3v3, d.VictoriesSolo, d.VictoriesDuo,
					d.ClubTag, d.ClubName, d.BrawlersData, d.Tag)
			}
		case "brawler":
			d := op.data.(brawlerData)
			s := prepare("brawler", `
				INSERT INTO player_brawlers (player_tag, brawler_id, brawler_name, trophies, skin_id, skin_name)
				VALUES (?,?,?,?,?,?)
				ON CONFLICT(player_tag, brawler_id) DO UPDATE SET
					trophies=excluded.trophies, skin_id=excluded.skin_id, skin_name=excluded.skin_name`)
			if s != nil {
				s.Exec(d.PlayerTag, d.BrawlerID, d.BrawlerName, d.Trophies, d.SkinID, d.SkinName)
			}
		case "brawler_stat":
			d := op.data.(brawlerStatData)
			s := prepare("brawler_stat", `
				INSERT INTO brawler_stats (brawler_id, brawler_name, player_count) VALUES (?,?,1)
				ON CONFLICT(brawler_id) DO UPDATE SET
					player_count=player_count+1, brawler_name=excluded.brawler_name`)
			if s != nil {
				s.Exec(d.BrawlerID, d.BrawlerName)
			}
		case "build":
			d := op.data.(buildData)
			s := prepare("build", `
				INSERT INTO brawler_build_stats (brawler_id, item_id, item_type, item_name) VALUES (?,?,?,?)
				ON CONFLICT(brawler_id, item_id) DO UPDATE SET equip_count=equip_count+1`)
			if s != nil {
				s.Exec(d.BrawlerID, d.ItemID, d.ItemType, d.ItemName)
			}
		case "match":
			d := op.data.(matchData)
			s := prepare("match", `
				INSERT OR IGNORE INTO matches
				(match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id, mode_id)
				VALUES (?,?,?,?,?,?,?,?,?,?)`)
			if s != nil {
				s.Exec(d.MatchID, d.BattleTime, d.Mode, d.Type, d.Map, d.MapID, d.Duration, d.StarPlayerTag, d.EventID, d.ModeID)
			}
		case "match_player":
			d := op.data.(matchPlayerData)
			s := prepare("match_player", `
				INSERT OR IGNORE INTO match_players
				(match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies,
				 skin_name, skin_id, is_winner, team_id, trophy_change, result, mode, map, match_type)
				VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
			if s != nil {
				s.Exec(d.MatchID, d.PlayerTag, d.BrawlerName, d.BrawlerID, d.BrawlerPower, d.BrawlerTrophies,
					d.SkinName, d.SkinID, d.IsWinner, d.TeamID, d.TrophyChange, d.Result, d.Mode, d.Map, d.MatchType)
			}
		case "status_404":
			tag := op.data.(string)
			s := prepare("status_404", `UPDATE players SET profile_updated_at=CURRENT_TIMESTAMP, is_processed=1 WHERE tag=?`)
			if s != nil {
				s.Exec(tag)
			}
		case "scan_done":
			tag := op.data.(string)
			s := prepare("scan_done", `UPDATE players SET last_battlelog_scan=CURRENT_TIMESTAMP WHERE tag=?`)
			if s != nil {
				s.Exec(tag)
			}
		case "new_tag":
			tag := op.data.(string)
			s := prepare("new_tag", `INSERT OR IGNORE INTO players (tag) VALUES (?)`)
			if s != nil {
				s.Exec(tag)
			}
		}
	}

	for _, s := range stmts {
		s.Close()
	}

	if err := tx.Commit(); err != nil {
		fmt.Println("[DB] Commit error:", err)
		tx.Rollback()
	}
}

// ─── Reporter ────────────────────────────────────────────────────────────────

func reporter() {
	ticker := time.NewTicker(5 * time.Second)
	var lastReq, lastEnrich uint64
	for range ticker.C {
		req     := atomic.LoadUint64(&totalRequests)
		enr     := atomic.LoadUint64(&enriched)
		disc    := atomic.LoadUint64(&discovered)
		r429    := atomic.LoadUint64(&errors429)
		rOther  := atomic.LoadUint64(&errorsOther)

		reqRate := (req - lastReq) / 5
		enrRate := (enr - lastEnrich) / 5
		lastReq = req
		lastEnrich = enr

		fmt.Printf("[NET %3d req/s] ENRICH: %d/s (total %d) | DISCOV: %d | 429: %d | ERR: %d\n",
			reqRate, enrRate, enr, disc, r429, rOther)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func doGET(client *http.Client, url, token string) ([]byte, int) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, 0
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 {
		return nil, 429
	}
	if resp.StatusCode != 200 {
		return nil, resp.StatusCode
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0
	}
	return body, 200
}

func cleanTag(tag string) string {
	t := strings.TrimPrefix(tag, "#")
	return strings.ToUpper(t)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
