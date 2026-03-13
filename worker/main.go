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

	// Start concurrent workers
	for w := 1; w <= poolSize; w++ {
		wg.Add(1)
		go worker(jobs, &wg, client, apiToken, db)
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
	Tag     string `json:"tag"`
	Name    string `json:"name"`
	Icon    struct {
		ID int `json:"id"`
	} `json:"icon"`
	Brawler struct {
		ID       int    `json:"id"`
		Name     string `json:"name"`
		Power    int    `json:"power"`
		Trophies int    `json:"trophies"`
		Skin     struct {
			Name string `json:"name"`
		} `json:"skin"`
	} `json:"brawler"`
	IsWinner int `json:"-"`
	TeamID   int `json:"-"`
}

type BattleEntry struct {
	BattleTime string `json:"battleTime"`
	Event      struct {
		ID   int    `json:"id"`
		Mode string `json:"mode"`
		Map  string `json:"map"`
	} `json:"event"`
	Battle struct {
		Mode       string         `json:"mode"`
		Type       string         `json:"type"`
		Result     string         `json:"result"`
		Duration   int            `json:"duration"`
		Rank       int            `json:"rank"`
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

func worker(jobs <-chan string, wg *sync.WaitGroup, client *http.Client, token string, db *sql.DB) {
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
				processSnowball(logData, db)
			}
			// Mark as processed
			_, _ = db.Exec("UPDATE players SET is_processed = 1 WHERE tag = ?", tag)
		default:
			atomic.AddUint64(&errorsHit, 1)
		}

		resp.Body.Close()
	}
}

func processSnowball(data BattleLogResponse, db *sql.DB) {
	today := time.Now().UTC().Format("20060102")
	discoveredTags := make(map[string]bool)

	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	stmtMatch, _ := tx.Prepare("INSERT OR IGNORE INTO matches (match_id, battle_time, mode, type, map, map_id, duration, star_player_tag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	stmtPlayer, _ := tx.Prepare("INSERT OR IGNORE INTO match_players (match_id, player_tag, player_name, brawler_name, brawler_id, brawler_power, brawler_trophies, skin_name, is_winner, team_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

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
		case "defeat":
			winnerTeam = 1
		}

		var matchPlayers []PlayerInfo
		var allTags []string

		for i, team := range item.Battle.Teams {
			for idx := range team {
				team[idx].TeamID = i
				if i == winnerTeam {
					team[idx].IsWinner = 1
				} else {
					team[idx].IsWinner = 0
				}
				allTags = append(allTags, team[idx].Tag)
			}
			matchPlayers = append(matchPlayers, team...)
		}
		
		for idx := range item.Battle.Players {
			item.Battle.Players[idx].TeamID = 0
			if item.Battle.Result == "victory" || item.Battle.Rank <= 4 {
				item.Battle.Players[idx].IsWinner = 1
			}
			allTags = append(allTags, item.Battle.Players[idx].Tag)
		}
		matchPlayers = append(matchPlayers, item.Battle.Players...)

		// 100% Unique Match ID Implementation
		// Sort tags alphabetically to ensure consistent hash regardless of player order
		sort.Strings(allTags)
		allTagsStr := strings.Join(allTags, ",")
		matchID := fmt.Sprintf("%s-%x", item.BattleTime, sha1.Sum([]byte(allTagsStr+item.Event.Map)))

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

			_, _ = stmtMatch.Exec(matchID, item.BattleTime, mode, item.Battle.Type, item.Event.Map, item.Event.ID, item.Battle.Duration, starPlayerTag)

			for _, p := range matchPlayers {
				if len(p.Tag) < 2 {
					continue
				}
				// Normalize Tag
				cleanTag := strings.TrimPrefix(p.Tag, "#")
				cleanTag = strings.ToUpper(cleanTag)
				
				_, _ = stmtPlayer.Exec(matchID, cleanTag, p.Name, p.Icon.ID, p.Brawler.Name, p.Brawler.ID, p.Brawler.Power, p.Brawler.Trophies, p.Brawler.Skin.Name, p.IsWinner, p.TeamID)
			}
		}
	}
	stmtMatch.Close()
	stmtPlayer.Close()

	if len(discoveredTags) > 0 {
		stmtNewTag, _ := tx.Prepare("INSERT OR IGNORE INTO players (tag) VALUES (?)")
		for t := range discoveredTags {
			cleanTag := strings.TrimPrefix(t, "#")
			cleanTag = strings.ToUpper(cleanTag)
			_, _ = stmtNewTag.Exec(cleanTag)
		}
		stmtNewTag.Close()
	}

	tx.Commit()
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
