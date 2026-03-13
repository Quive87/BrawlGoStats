package main

import (
	"crypto/sha1"
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
	_ "modernc.org/sqlite" // Pure Go SQLite (No CGO required)
)

const (
	dbPath        = "../brawl_data.sqlite"
	baseURL       = "https://api.brawlstars.com/v1"
	poolSize      = 64   // Increased concurrent workers
	batchLoadSize = 1000 // Larger batch to reduce DB contention
)

type Payload struct {
	Match  []interface{} // Match data args: match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id
	Player []interface{} // Match_Player data args: match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies, skin_name, skin_id, is_winner, team_id, trophy_change, result
	Upsert []interface{} // Player upsert args: tag, name
	NewTag string
}

var (
	tagsProcessed uint64
	rateLimitsHit uint64
	errorsHit     uint64
)

func main() {
	_ = godotenv.Load("../.env")
	apiToken := os.Getenv("SUPERCELL_API_TOKEN")
	if apiToken == "" {
		fmt.Println("ERROR: SUPERCELL_API_TOKEN not found in .env")
		os.Exit(1)
	}

	// Connect to shared SQLite with WAL mode
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_sync=NORMAL")
	if err != nil {
		fmt.Printf("DB Connect Error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Prep the job channel (Buffered for high-throughput)
	jobs := make(chan string, batchLoadSize*2)
	var wg sync.WaitGroup

	// High-throughput HTTP client
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        poolSize,
			MaxIdleConnsPerHost: poolSize,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	// Start DB Writer
	dbWriterChan := make(chan Payload, 5000)
	var dbWg sync.WaitGroup
	dbWg.Add(1)
	go databaseWriter(db, dbWriterChan, &dbWg)

	// Start concurrent workers
	for w := 1; w <= poolSize; w++ {
		wg.Add(1)
		go worker(jobs, &wg, client, apiToken, dbWriterChan, db)
	}

	// Start Metrics Reporter
	go reporter()

	fmt.Printf("--- Native High-Speed Worker Started (%d workers) --- \n", poolSize)

	// Shutdown listener
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sigchan:
			fmt.Println("Shutting down...")
			close(jobs)
			wg.Wait()
			close(dbWriterChan)
			dbWg.Wait()
			return
		default:
			// Fill the jobs queue if it's running low (Reduce starvation)
			if len(jobs) < poolSize {
				tags := fetchUnprocessedTags(db, batchLoadSize)
				if len(tags) == 0 {
					time.Sleep(2 * time.Second) // Wait for Python scraper
					continue
				}
				for _, t := range tags {
					jobs <- t
				}
			}
			time.Sleep(50 * time.Millisecond) // More responsive loop
		}
	}
}

func fetchUnprocessedTags(db *sql.DB, limit int) []string {
	rows, err := db.Query("SELECT tag FROM players WHERE is_processed = 0 LIMIT ?", limit)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err == nil {
			tags = append(tags, tag)
		}
	}
	return tags
}

type PlayerInfo struct {
	Tag  string `json:"tag"`
	Name string `json:"name"`
	Brawler struct {
		ID       int    `json:"id"`
		Name     string `json:"name"`
		Power    int    `json:"power"`
		Trophies int    `json:"trophies"`
		Skin     struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		} `json:"skin"`
	} `json:"brawler"`
	TrophyChange int    `json:"trophyChange"`
	Result       string `json:"-"`
	IsWinner     int    `json:"-"`
	TeamID       int    `json:"-"`
}

type BattleEntry struct {
	BattleTime string `json:"battleTime"`
	Event      struct {
		ID   int    `json:"id"`
		Mode string `json:"mode"`
		Map  string `json:"map"`
	} `json:"event"`
	Battle struct {
		Mode       string `json:"mode"`
		Type       string `json:"type"`
		Result     string `json:"result"`
		Duration   int    `json:"duration"`
		Rank       int    `json:"rank"`
		StarPlayer struct {
			Tag string `json:"tag"`
		} `json:"starPlayer"`
		Teams   [][]PlayerInfo `json:"teams"`
		Players []PlayerInfo   `json:"players"`
	} `json:"battle"`
}

type BattleLogResponse struct {
	Items []BattleEntry `json:"items"`
}


func worker(jobs <-chan string, wg *sync.WaitGroup, client *http.Client, token string, out chan<- Payload, db *sql.DB) {
	defer wg.Done()

	for tag := range jobs {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/players/%%23%s/battlelog", baseURL, tag), nil)
		req.Header.Add("Authorization", "Bearer "+token)

		resp, err := client.Do(req)
		if err != nil {
			atomic.AddUint64(&errorsHit, 1)
			continue
		}

		atomic.AddUint64(&tagsProcessed, 1)

		switch resp.StatusCode {
		case 429:
			atomic.AddUint64(&rateLimitsHit, 1)
			time.Sleep(5 * time.Second)
		case 200:
			body, _ := io.ReadAll(resp.Body)
			var logData BattleLogResponse
			if err := json.Unmarshal(body, &logData); err == nil {
				processSnowball(logData, out)
			}
			// Mark as processed
			_, _ = db.Exec("UPDATE players SET is_processed = 1 WHERE tag = ?", tag)
		default:
			atomic.AddUint64(&errorsHit, 1)
		}

		resp.Body.Close()
	}
}

func processSnowball(data BattleLogResponse, out chan<- Payload) {
	today := time.Now().UTC().Format("20060102")
	discoveredTags := make(map[string]bool)

	for _, item := range data.Items {
		isToday := strings.HasPrefix(item.BattleTime, today)

		mode := item.Event.Mode
		if mode == "" {
			mode = item.Battle.Mode
		}

		// Determine winner
		winnerTeam := -1
		switch item.Battle.Result {
		case "victory":
			winnerTeam = 0
			item.Battle.Result = "victory"
		case "defeat":
			winnerTeam = 1
			item.Battle.Result = "defeat"
		}

		var matchPlayers []PlayerInfo
		var allTags []string

		for i, team := range item.Battle.Teams {
			for idx := range team {
				team[idx].TeamID = i
				if i == winnerTeam {
					team[idx].IsWinner = 1
					team[idx].Result = "victory"
				} else {
					team[idx].IsWinner = 0
					team[idx].Result = "defeat"
				}
				allTags = append(allTags, team[idx].Tag)
			}
			matchPlayers = append(matchPlayers, team...)
		}

		for idx := range item.Battle.Players {
			item.Battle.Players[idx].TeamID = 0
			if item.Battle.Result == "victory" || item.Battle.Rank <= 4 {
				item.Battle.Players[idx].IsWinner = 1
				item.Battle.Players[idx].Result = "victory"
			} else {
				item.Battle.Players[idx].Result = "defeat"
			}
			allTags = append(allTags, item.Battle.Players[idx].Tag)
		}
		matchPlayers = append(matchPlayers, item.Battle.Players...)

		// 100% Unique Match ID Implementation
		// Include Event ID and Duration to prevent collisions on same-time matches
		sort.Strings(allTags)
		allTagsStr := strings.Join(allTags, ",")
		matchID := fmt.Sprintf("%s-%x", item.BattleTime, sha1.Sum([]byte(fmt.Sprintf("%s|%d|%d|%s", allTagsStr, item.Event.ID, item.Battle.Duration, item.Event.Map))))

		// 1. Snowball Discovery: Always collect tags from EVERY match
		for _, p := range matchPlayers {
			if len(p.Tag) >= 2 {
				discoveredTags[p.Tag] = true
			}
		}

		// 2. Selective Storage: Only save matches from today
		if isToday {
			// Normalize Star Player Tag
			starPlayerTag := strings.TrimPrefix(item.Battle.StarPlayer.Tag, "#")
			starPlayerTag = strings.ToUpper(starPlayerTag)

			out <- Payload{Match: []any{matchID, item.BattleTime, mode, item.Battle.Type, item.Event.Map, item.Event.ID, item.Battle.Duration, starPlayerTag, item.Event.ID}}

			for _, p := range matchPlayers {
				if len(p.Tag) < 2 {
					continue
				}
				// Normalize Tag
				cleanTag := strings.TrimPrefix(p.Tag, "#")
				cleanTag = strings.ToUpper(cleanTag)

				out <- Payload{Player: []any{matchID, cleanTag, p.Brawler.Name, p.Brawler.ID, p.Brawler.Power, p.Brawler.Trophies, p.Brawler.Skin.Name, p.Brawler.Skin.ID, p.IsWinner, p.TeamID, p.TrophyChange, p.Result}}
				out <- Payload{Upsert: []any{cleanTag, p.Name}}
			}
		}
	}

	if len(discoveredTags) > 0 {
		for t := range discoveredTags {
			cleanTag := strings.TrimPrefix(t, "#")
			cleanTag = strings.ToUpper(cleanTag)
			out <- Payload{NewTag: cleanTag}
		}
	}
}

// Single goroutine that batches DB writes to prevent SQL locks
func databaseWriter(db *sql.DB, in <-chan Payload, wg *sync.WaitGroup) {
	defer wg.Done()
	var batch []Payload
	ticker := time.NewTicker(2 * time.Second) // Commit every 2 seconds or 500 records
	defer ticker.Stop()

	for {
		select {
		case p, ok := <-in:
			if !ok {
				flushBatch(db, batch)
				return
			}
			batch = append(batch, p)
			if len(batch) >= 500 { // Bulk insert threshold
				flushBatch(db, batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(db, batch)
				batch = batch[:0]
			}
		}
	}
}

func flushBatch(db *sql.DB, batch []Payload) {
	tx, err := db.Begin()
	if err != nil {
		fmt.Printf("TX Begin Error: %v\n", err)
		return
	}

	stmtMatch, _ := tx.Prepare("INSERT OR IGNORE INTO matches (match_id, battle_time, mode, type, map, map_id, duration, star_player_tag, event_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	stmtPlayer, _ := tx.Prepare("INSERT OR IGNORE INTO match_players (match_id, player_tag, brawler_name, brawler_id, brawler_power, brawler_trophies, skin_name, skin_id, is_winner, team_id, trophy_change, result) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	stmtUpsert, _ := tx.Prepare(`
		INSERT INTO players (tag, name) 
		VALUES (?, ?) 
		ON CONFLICT(tag) DO UPDATE SET 
			name = excluded.name 
		WHERE name IS NULL OR name = ''
	`)
	stmtNewTag, _ := tx.Prepare("INSERT OR IGNORE INTO players (tag) VALUES (?)")

	for _, p := range batch {
		if p.Match != nil {
			_, _ = stmtMatch.Exec(p.Match...)
		}
		if p.Player != nil {
			_, _ = stmtPlayer.Exec(p.Player...)
		}
		if p.Upsert != nil {
			_, _ = stmtUpsert.Exec(p.Upsert...)
		}
		if p.NewTag != "" {
			_, _ = stmtNewTag.Exec(p.NewTag)
		}
	}

	_ = stmtMatch.Close()
	_ = stmtPlayer.Close()
	_ = stmtUpsert.Close()
	_ = stmtNewTag.Close()
	_ = tx.Commit()
}

func reporter() {
	ticker := time.NewTicker(5 * time.Second)
	var lastProcessed uint64
	for range ticker.C {
		curr := atomic.LoadUint64(&tagsProcessed)
		rate := (curr - lastProcessed) / 5
		lastProcessed = curr
		fmt.Printf("[NET %3d req/s] Total: %-6d | 429: %-4d | Errors: %-4d\n",
			rate, curr, atomic.LoadUint64(&rateLimitsHit), atomic.LoadUint64(&errorsHit))
	}
}
