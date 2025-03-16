package main

import (
	"cloud.google.com/go/firestore"
	memorystore "cloud.google.com/go/redis/apiv1"
	"cloud.google.com/go/redis/apiv1/redispb"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	firebase "firebase.google.com/go"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Define log levels
const (
	LogLevelInfo    = "INFO"
	LogLevelWarning = "WARNING"
	LogLevelError   = "ERROR"
	LogLevelDebug   = "DEBUG"
	KeepaRateLimit  = 100 // Adjust this based on your Keepa API plan
)

// KeepaResponse represents the structure of the Keepa API response
type KeepaResponse struct {
	Timestamp          int64          `json:"timestamp"`
	TokensLeft         int            `json:"tokensLeft"`
	RefillIn           int            `json:"refillIn"`
	RefillRate         int            `json:"refillRate"`
	TokenFlowReduction float64        `json:"tokenFlowReduction"`
	TokensConsumed     int            `json:"tokensConsumed"`
	ProcessingTimeInMs int            `json:"processingTimeInMs"`
	Products           []KeepaProduct `json:"products"`
}

// Offer represents a single marketplace offer
type Offer struct {
	LastSeen         int         `json:"lastSeen"`
	SellerID         string      `json:"sellerId"`
	OfferCSV         []int       `json:"offerCSV"`
	Condition        int         `json:"condition"`
	ConditionComment interface{} `json:"conditionComment"`
	IsPrime          bool        `json:"isPrime"`
	IsMAP            bool        `json:"isMAP"`
	IsShippable      bool        `json:"isShippable"`
	IsAddonItem      bool        `json:"isAddonItem"`
	IsPreorder       bool        `json:"isPreorder"`
	IsWarehouseDeal  bool        `json:"isWarehouseDeal"`
	IsScam           bool        `json:"isScam"`
	IsAmazon         bool        `json:"isAmazon"`
	IsPrimeExcl      bool        `json:"isPrimeExcl"`
	OfferID          int         `json:"offerId"`
	StockCSV         []int       `json:"stockCSV"`
	IsFBA            bool        `json:"isFBA"`
	ShipsFromChina   bool        `json:"shipsFromChina"`
	StockLimit       []int       `json:"stockLimit"`
	MinOrderQty      int         `json:"minOrderQty"`
	CouponHistory    []int       `json:"couponHistory"`
}

// FBAFees represents Amazon FBA fees
type FBAFees struct {
	LastUpdate     int `json:"lastUpdate"`
	PickAndPackFee int `json:"pickAndPackFee"`
}

// Variation represents product variations
type Variation struct {
	Asin       string      `json:"asin"`
	Attributes []Attribute `json:"attributes"`
}

// Attribute represents a variation attribute
type Attribute struct {
	Dimension string `json:"dimension"`
	Value     string `json:"value"`
}

// UnitCount represents product unit information
type UnitCount struct {
	UnitValue float64 `json:"unitValue"`
	UnitType  string  `json:"unitType"`
}

// CategoryTreeItem represents an item in the category hierarchy
type CategoryTreeItem struct {
	CatID int    `json:"catId"`
	Name  string `json:"name"`
}

// BuyBoxSellerStats represents statistics for a seller in the buy box
type BuyBoxSellerStats struct {
	PercentageWon     float64 `json:"percentageWon"`
	AvgPrice          int     `json:"avgPrice"`
	AvgNewOfferCount  int     `json:"avgNewOfferCount"`
	AvgUsedOfferCount int     `json:"avgUsedOfferCount"`
	IsFBA             bool    `json:"isFBA"`
	LastSeen          int     `json:"lastSeen"`
	Condition         int     `json:"condition,omitempty"` // Only used in BuyBoxUsedStats
}

// ProductStats represents statistics for the product
type ProductStats struct {
	Current                        []int                        `json:"current"`
	Avg                            []int                        `json:"avg"`
	Avg30                          []int                        `json:"avg30"`
	Avg90                          []int                        `json:"avg90"`
	Avg180                         []int                        `json:"avg180"`
	Avg365                         []int                        `json:"avg365"`
	AtIntervalStart                []int                        `json:"atIntervalStart"`
	Min                            []interface{}                `json:"min"`
	Max                            []interface{}                `json:"max"`
	MinInInterval                  []interface{}                `json:"minInInterval"`
	MaxInInterval                  []interface{}                `json:"maxInInterval"`
	IsLowest                       []bool                       `json:"isLowest"`
	IsLowest90                     []bool                       `json:"isLowest90"`
	OutOfStockPercentageInInterval []int                        `json:"outOfStockPercentageInInterval"`
	OutOfStockPercentage365        []int                        `json:"outOfStockPercentage365"`
	OutOfStockPercentage180        []int                        `json:"outOfStockPercentage180"`
	OutOfStockPercentage90         []int                        `json:"outOfStockPercentage90"`
	OutOfStockPercentage30         []int                        `json:"outOfStockPercentage30"`
	OutOfStockCountAmazon30        int                          `json:"outOfStockCountAmazon30"`
	OutOfStockCountAmazon90        int                          `json:"outOfStockCountAmazon90"`
	DeltaPercent90MonthlySold      int                          `json:"deltaPercent90_monthlySold"`
	StockPerCondition3RdFBA        []int                        `json:"stockPerCondition3rdFBA"`
	StockPerConditionFBM           []int                        `json:"stockPerConditionFBM"`
	RetrievedOfferCount            int                          `json:"retrievedOfferCount"`
	TotalOfferCount                int                          `json:"totalOfferCount"`
	TradeInPrice                   int                          `json:"tradeInPrice"`
	LastOffersUpdate               int                          `json:"lastOffersUpdate"`
	IsAddonItem                    bool                         `json:"isAddonItem"`
	LightningDealInfo              interface{}                  `json:"lightningDealInfo"`
	SellerIdsLowestFBA             []string                     `json:"sellerIdsLowestFBA"`
	SellerIdsLowestFBM             []string                     `json:"sellerIdsLowestFBM"`
	OfferCountFBA                  int                          `json:"offerCountFBA"`
	OfferCountFBM                  int                          `json:"offerCountFBM"`
	SalesRankDrops30               int                          `json:"salesRankDrops30"`
	SalesRankDrops90               int                          `json:"salesRankDrops90"`
	SalesRankDrops180              int                          `json:"salesRankDrops180"`
	SalesRankDrops365              int                          `json:"salesRankDrops365"`
	BuyBoxPrice                    int                          `json:"buyBoxPrice"`
	BuyBoxShipping                 int                          `json:"buyBoxShipping"`
	BuyBoxIsUnqualified            bool                         `json:"buyBoxIsUnqualified"`
	BuyBoxIsShippable              bool                         `json:"buyBoxIsShippable"`
	BuyBoxIsPreorder               bool                         `json:"buyBoxIsPreorder"`
	BuyBoxIsFBA                    bool                         `json:"buyBoxIsFBA"`
	BuyBoxIsAmazon                 bool                         `json:"buyBoxIsAmazon"`
	BuyBoxIsMAP                    bool                         `json:"buyBoxIsMAP"`
	BuyBoxIsUsed                   bool                         `json:"buyBoxIsUsed"`
	BuyBoxIsBackorder              bool                         `json:"buyBoxIsBackorder"`
	BuyBoxIsPrimeExclusive         bool                         `json:"buyBoxIsPrimeExclusive"`
	BuyBoxIsFreeShippingEligible   bool                         `json:"buyBoxIsFreeShippingEligible"`
	BuyBoxIsPrimePantry            bool                         `json:"buyBoxIsPrimePantry"`
	BuyBoxIsPrimeEligible          bool                         `json:"buyBoxIsPrimeEligible"`
	BuyBoxMinOrderQuantity         int                          `json:"buyBoxMinOrderQuantity"`
	BuyBoxMaxOrderQuantity         int                          `json:"buyBoxMaxOrderQuantity"`
	BuyBoxCondition                int                          `json:"buyBoxCondition"`
	LastBuyBoxUpdate               int                          `json:"lastBuyBoxUpdate"`
	BuyBoxAvailabilityMessage      interface{}                  `json:"buyBoxAvailabilityMessage"`
	BuyBoxShippingCountry          interface{}                  `json:"buyBoxShippingCountry"`
	BuyBoxSellerID                 string                       `json:"buyBoxSellerId"`
	BuyBoxIsWarehouseDeal          bool                         `json:"buyBoxIsWarehouseDeal"`
	BuyBoxStats                    map[string]BuyBoxSellerStats `json:"buyBoxStats"`
	BuyBoxUsedStats                map[string]BuyBoxSellerStats `json:"buyBoxUsedStats"`
}

// AutoGenerated is the main product data structure
type KeepaProduct struct {
	Csv                             []interface{}      `json:"csv"`
	Categories                      []int64            `json:"categories"`
	ImagesCSV                       string             `json:"imagesCSV"`
	Manufacturer                    string             `json:"manufacturer"`
	Title                           string             `json:"title"`
	LastUpdate                      int                `json:"lastUpdate"`
	LastPriceChange                 int                `json:"lastPriceChange"`
	RootCategory                    int                `json:"rootCategory"`
	ProductType                     int                `json:"productType"`
	ParentAsin                      string             `json:"parentAsin"`
	VariationCSV                    string             `json:"variationCSV"`
	Asin                            string             `json:"asin"`
	DomainID                        int                `json:"domainId"`
	Type                            string             `json:"type"`
	HasReviews                      bool               `json:"hasReviews"`
	TrackingSince                   int                `json:"trackingSince"`
	Brand                           string             `json:"brand"`
	ProductGroup                    string             `json:"productGroup"`
	PartNumber                      string             `json:"partNumber"`
	Model                           string             `json:"model"`
	Color                           string             `json:"color"`
	Size                            string             `json:"size"`
	Edition                         interface{}        `json:"edition"`
	Format                          interface{}        `json:"format"`
	PackageHeight                   int                `json:"packageHeight"`
	PackageLength                   int                `json:"packageLength"`
	PackageWidth                    int                `json:"packageWidth"`
	PackageWeight                   int                `json:"packageWeight"`
	PackageQuantity                 int                `json:"packageQuantity"`
	IsAdultProduct                  bool               `json:"isAdultProduct"`
	IsEligibleForTradeIn            bool               `json:"isEligibleForTradeIn"`
	IsEligibleForSuperSaverShipping bool               `json:"isEligibleForSuperSaverShipping"`
	Offers                          []Offer            `json:"offers"`
	BuyBoxSellerIDHistory           []string           `json:"buyBoxSellerIdHistory"`
	IsRedirectASIN                  bool               `json:"isRedirectASIN"`
	IsSNS                           bool               `json:"isSNS"`
	Author                          interface{}        `json:"author"`
	Binding                         string             `json:"binding"`
	NumberOfItems                   int                `json:"numberOfItems"`
	NumberOfPages                   int                `json:"numberOfPages"`
	PublicationDate                 int                `json:"publicationDate"`
	ReleaseDate                     int                `json:"releaseDate"`
	Languages                       interface{}        `json:"languages"`
	LastRatingUpdate                int                `json:"lastRatingUpdate"`
	EbayListingIds                  interface{}        `json:"ebayListingIds"`
	LastEbayUpdate                  int                `json:"lastEbayUpdate"`
	EanList                         []string           `json:"eanList"`
	UpcList                         []string           `json:"upcList"`
	LiveOffersOrder                 []int              `json:"liveOffersOrder"`
	FrequentlyBoughtTogether        []string           `json:"frequentlyBoughtTogether"`
	Features                        []string           `json:"features"`
	Description                     string             `json:"description"`
	Promotions                      interface{}        `json:"promotions"`
	NewPriceIsMAP                   bool               `json:"newPriceIsMAP"`
	Coupon                          interface{}        `json:"coupon"`
	AvailabilityAmazon              int                `json:"availabilityAmazon"`
	ListedSince                     int                `json:"listedSince"`
	FbaFees                         FBAFees            `json:"fbaFees"`
	Variations                      []Variation        `json:"variations"`
	ItemHeight                      int                `json:"itemHeight"`
	ItemLength                      int                `json:"itemLength"`
	ItemWidth                       int                `json:"itemWidth"`
	ItemWeight                      int                `json:"itemWeight"`
	SalesRankReference              int                `json:"salesRankReference"`
	SalesRanks                      map[string][]int   `json:"salesRanks"`
	SalesRankReferenceHistory       []int              `json:"salesRankReferenceHistory"`
	Launchpad                       bool               `json:"launchpad"`
	IsB2B                           bool               `json:"isB2B"`
	LastStockUpdate                 int                `json:"lastStockUpdate"`
	BuyBoxUsedHistory               []string           `json:"buyBoxUsedHistory"`
	LastSoldUpdate                  int                `json:"lastSoldUpdate"`
	MonthlySold                     int                `json:"monthlySold"`
	MonthlySoldHistory              []int              `json:"monthlySoldHistory"`
	BuyBoxEligibleOfferCounts       []int              `json:"buyBoxEligibleOfferCounts"`
	CompetitivePriceThreshold       int                `json:"competitivePriceThreshold"`
	ParentAsinHistory               []string           `json:"parentAsinHistory"`
	IsHeatSensitive                 bool               `json:"isHeatSensitive"`
	ReturnRate                      int                `json:"returnRate"`
	URLSlug                         string             `json:"urlSlug"`
	UnitCount                       UnitCount          `json:"unitCount"`
	ItemTypeKeyword                 string             `json:"itemTypeKeyword"`
	RecommendedUsesForProduct       string             `json:"recommendedUsesForProduct"`
	Style                           string             `json:"style"`
	IncludedComponents              string             `json:"includedComponents"`
	Material                        string             `json:"material"`
	BrandStoreName                  string             `json:"brandStoreName"`
	BrandStoreURL                   string             `json:"brandStoreUrl"`
	Stats                           ProductStats       `json:"stats"`
	OffersSuccessful                bool               `json:"offersSuccessful"`
	G                               int                `json:"g"`
	CategoryTree                    []CategoryTreeItem `json:"categoryTree"`
	ParentTitle                     string             `json:"parentTitle"`
	BrandStoreURLName               string             `json:"brandStoreUrlName"`
	ReferralFeePercent              int                `json:"referralFeePercent"`
	ReferralFeePercentage           float64            `json:"referralFeePercentage"`
}

// Create simplified response with only the needed fields
type SimplifiedOffer struct {
	SellerID  string         `json:"sellerId"`
	Condition int            `json:"condition"`
	IsPrime   bool           `json:"isPrime"`
	IsAmazon  bool           `json:"isAmazon"`
	IsFBA     bool           `json:"isFBA"`
	StockCSV  map[string]int `json:"stockCSV,omitempty"`
}

type SimplifiedProduct struct {
	Asin        string            `json:"asin"`
	Title       string            `json:"title"`
	Categories  []int64           `json:"categories"`
	Brand       string            `json:"brand"`
	BuyBoxPrice int               `json:"buyBoxPrice,omitempty"`
	SalesRanks  map[string]int    `json:"salesRanks,omitempty"`
	Offers      []SimplifiedOffer `json:"offers,omitempty"`
}

type SimplifiedResponse struct {
	Products []SimplifiedProduct `json:"products"`
}

// ASINRequest represents the structure of the incoming POST request
type ASINRequest struct {
	ASINs []string `json:"asins"`
}

// Add these constants for Redis
const (
	// ... existing constants
	RedisKeyPrefix = "keepa:product:"
	RedisTTL       = 24 * time.Hour
)

// Add Redis client as a global variable
var redisClient *redis.Client

// Add Firestore client as a global variable
var firestoreClient *firestore.Client

// Logger instance
var logger *log.Logger

var keepaLimiter *rate.Limiter

func init() {
	// Create log file if it doesn't exist
	logFile := getEnvWithDefault("LOG_FILE", "keepa_api.log")
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Create multi-output logger to write to both console and file
	multiWriter := io.MultiWriter(os.Stdout, file)
	logger = log.New(multiWriter, "", log.Ldate|log.Ltime|log.Lmicroseconds)

	logMessage(LogLevelInfo, "Logging system initialized")

	// Initialize Redis client
	redisAddr := getEnvWithDefault("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnvWithDefault("REDIS_PASSWORD", "")
	redisDB, _ := strconv.Atoi(getEnvWithDefault("REDIS_DB", "0"))
	projectID := getEnvWithDefault("PROJECT_ID", "")
	location := getEnvWithDefault("REGION", "")
	instanceID := getEnvWithDefault("INSTANCE_ID", "")
	// Configure Redis options
	ctx := context.Background()

	adminClient, err := memorystore.NewCloudRedisClient(ctx)
	if err != nil {
		logMessage(LogLevelError, "Failed to create redis client: %v", err)
	}
	defer adminClient.Close()

	req := &redispb.GetInstanceRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/instances/%s", projectID, location, instanceID),
	}

	instance, err := adminClient.GetInstance(ctx, req)
	if err != nil {
		logMessage(LogLevelError, "Failed to get instance: %v", err)
	}

	// Load CA cert
	caCerts := instance.GetServerCaCerts()
	if len(caCerts) == 0 {
		logMessage(LogLevelError, "Failed to get instance caCerts")
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(caCerts[0].Cert))

	redisOptions := &redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           redisDB,
		PoolSize:     10,              // 连接池大小
		MinIdleConns: 2,               // 最小空闲连接数
		DialTimeout:  5 * time.Second, // 连接超时
		ReadTimeout:  3 * time.Second, // 读取超时
		WriteTimeout: 3 * time.Second, // 写入超时
		PoolTimeout:  4 * time.Second, // 获取连接的超时时间
		TLSConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	}

	redisClient = redis.NewClient(redisOptions)

	// Test Redis connection

	_, err = redisClient.Ping(ctx).Result()

	// 启动健康检查 goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second) // 每 30 秒检查一次
		defer ticker.Stop()

		for range ticker.C {
			err = redisClient.Ping(ctx).Err()
			if err != nil {
				logMessage(LogLevelWarning, "Failed to connect to Redis: %v", err)
			} else {
				logMessage(LogLevelInfo, "Connected to Redis at %s", redisAddr)
			}
		}
	}()

	// Initialize rate limiter
	keepaLimiter = rate.NewLimiter(rate.Limit(KeepaRateLimit), KeepaRateLimit)

	// Initialize Firestore client
	conf := &firebase.Config{ProjectID: projectID}
	app, err := firebase.NewApp(ctx, conf)
	if err != nil {
		logMessage(LogLevelError, "Failed to initialize Firestore client: %v", err)
	}

	firestoreClient, err = app.Firestore(ctx)
	if err != nil {
		logMessage(LogLevelError, "Failed to initialize Firestore client: %v", err)
	}

	logMessage(LogLevelInfo, "Rate limiter initialized with limit of %d requests per second", KeepaRateLimit)
}

// Add these helper functions for Redis operations
func getProductFromRedis(ctx context.Context, asin string) (*SimplifiedResponse, error) {
	key := RedisKeyPrefix + asin
	data, err := redisClient.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("product not found in Redis")
	} else if err != nil {
		return nil, fmt.Errorf("failed to get product from Redis: %v", err)
	}
	var simplifiedResponse SimplifiedResponse
	err = json.Unmarshal(data, &simplifiedResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal product from Redis: %v", err)
	}
	return &simplifiedResponse, nil
}

func saveProductToRedis(ctx context.Context, asin string, simplifiedResponse *SimplifiedResponse) error {
	key := RedisKeyPrefix + asin
	data, _ := json.Marshal(simplifiedResponse)
	return redisClient.Set(ctx, key, data, RedisTTL).Err()
}

// Helper function to log messages
func logMessage(level string, format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)
	logger.Printf("[%s] %s", level, message)
}

func main() {
	logMessage(LogLevelInfo, "Service starting...")

	// Create a default gin router
	r := gin.Default()

	// Define a route to handle Keepa product API requests
	r.POST("/product", handleKeepaProduct)

	// Run the server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	logMessage(LogLevelInfo, "Server will start on port %s", port)
	r.Run(":" + port) // For Gin
}

// Function to handle Keepa product API requests
func handleKeepaProduct(c *gin.Context) {
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	clientIP := c.ClientIP()
	logMessage(LogLevelInfo, "[RequestID: %s] Received request from %s", requestID, clientIP)

	// Parse the JSON body
	var request ASINRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		logMessage(LogLevelError, "[RequestID: %s] Failed to parse request body: %v", requestID, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	asinList := request.ASINs
	if len(asinList) == 0 {
		logMessage(LogLevelWarning, "[RequestID: %s] No ASINs provided in request", requestID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "No ASINs provided"})
		return
	}

	logMessage(LogLevelInfo, "[RequestID: %s] Request parameters ASINs: %v", requestID, asinList)

	// Create a semaphore to limit concurrent API calls
	sem := make(chan struct{}, 10) // Adjust this value based on your needs

	// Process each ASIN
	for _, asin := range asinList {
		go func(asin string) {
			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Create a context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			// Try to get data from Redis first
			_, err := getProductFromRedis(ctx, asin)
			if err == nil {
				logMessage(LogLevelInfo, "[RequestID: %s] Cache hit for ASIN: %s", requestID, asin)
				return
			}

			// If not in cache, fetch from Keepa API
			productData, err := fetchFromKeepaAPI(ctx, requestID, asin)
			if err != nil {
				logMessage(LogLevelError, "[RequestID: %s] Failed to fetch data for ASIN %s: %v", requestID, asin, err)
				return
			}

			// delete product from Firestore
			if err := deleteFromFirestore(ctx, asin); err != nil {
				logMessage(LogLevelWarning, "[RequestID: %s] Failed to delete data from Firestore for ASIN %s: %v", requestID, asin, err)
			}

			// Save to Firestore
			if err = saveToFirestore(ctx, asin, productData); err != nil {
				logMessage(LogLevelWarning, "[RequestID: %s] Failed to save data to Firestore for ASIN %s: %v", requestID, asin, err)
			}

			// Save to Redis
			err = saveProductToRedis(ctx, asin, productData)
			if err != nil {
				logMessage(LogLevelWarning, "[RequestID: %s] Failed to save data to Redis for ASIN %s: %v", requestID, asin, err)
			}

		}(asin)
	}

	// Return the combined results
	c.JSON(http.StatusOK, nil)
}

func deleteFromFirestore(ctx context.Context, asin string) interface{} {
	// Delete product from Firestore
	docRef := firestoreClient.Collection("products").Doc(asin)
	_, err := docRef.Delete(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete product from Firestore: %v", err)
	}
	return nil
}

func saveToFirestore(ctx context.Context, asin string, productData *SimplifiedResponse) error {
	// Create a new document in Firestore
	docRef := firestoreClient.Collection("products").Doc(asin)
	_, err := docRef.Set(ctx, productData)
	if err != nil {
		return fmt.Errorf("failed to save product to Firestore: %v", err)
	}
	return nil
}

func fetchFromKeepaAPI(ctx context.Context, requestID, asin string) (*SimplifiedResponse, error) {
	// Maximum number of retries
	maxRetries := 3
	// Initial backoff duration
	backoffDuration := 30 * time.Second

	var keepaResponse KeepaResponse
	var res *http.Response
	var body []byte
	var err error

	// Wait for permission from the rate limiter
	if err := keepaLimiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit exceeded: %v", err)
	}

	// Read Keepa API parameters from environment variables
	domain := getEnvWithDefault("KEEPA_DOMAIN", "1")
	apiKey := getEnvWithDefault("KEEPA_API_KEY", "rt7t1904up7638ddhboifgfksfedu7pap6gde8p5to6mtripoib3q4n1h3433rh4")
	stats := getEnvWithDefault("KEEPA_STATS", "90")
	update := getEnvWithDefault("KEEPA_UPDATE", "-1")
	history := getEnvWithDefault("KEEPA_HISTORY", "1")
	days := getEnvWithDefault("KEEPA_DAYS", "90")
	codeLimit := getEnvWithDefault("KEEPA_CODE_LIMIT", "10")
	offers := getEnvWithDefault("KEEPA_OFFERS", "20")
	onlyLiveOffers := getEnvWithDefault("KEEPA_ONLY_LIVE_OFFERS", "1")
	rental := getEnvWithDefault("KEEPA_RENTAL", "0")
	videos := getEnvWithDefault("KEEPA_VIDEOS", "0")
	aplus := getEnvWithDefault("KEEPA_APLUS", "0")
	rating := getEnvWithDefault("KEEPA_RATING", "0")
	buybox := getEnvWithDefault("KEEPA_BUYBOX", "1")
	stock := getEnvWithDefault("KEEPA_STOCK", "1")

	// Build Keepa API URL
	url := fmt.Sprintf("https://api.keepa.com/product?domain=%s&key=%s&asin=%s&stats=%s&update=%s&history=%s&days=%s&code-limit=%s&offers=%s&only-live-offers=%s&rental=%s&videos=%s&aplus=%s&rating=%s&buybox=%s&stock=%s",
		domain, apiKey, asin, stats, update, history, days, codeLimit, offers, onlyLiveOffers, rental, videos, aplus, rating, buybox, stock)

	logMessage(LogLevelInfo, "[RequestID: %s] Preparing to send request to Keepa API for ASIN: %s", requestID, asin)
	logMessage(LogLevelDebug, "[RequestID: %s] Request URL: %s", requestID, url)

	// Create HTTP client
	client := &http.Client{
		Timeout: 120 * time.Second, // Set timeout
	}

	// Create request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Retry loop
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// If this is a retry attempt, log and wait
		if attempt > 0 {
			logMessage(LogLevelWarning, "[RequestID: %s] Retry attempt %d for ASIN %s after rate limit (429). Waiting %v before retry",
				requestID, attempt, asin, backoffDuration)

			select {
			case <-time.After(backoffDuration):
				// Continue after backoff
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled during backoff: %v", ctx.Err())
			}

			// Increase backoff for next potential retry (exponential backoff)
			backoffDuration *= 2
		}

		// Record request start time
		startTime := time.Now()

		// Send request
		res, err = client.Do(req)
		if err != nil {
			if attempt == maxRetries {
				return nil, fmt.Errorf("failed to send request after %d attempts: %v", maxRetries+1, err)
			}
			logMessage(LogLevelError, "[RequestID: %s] Failed to send request (attempt %d): %v", requestID, attempt+1, err)
			continue
		}

		// Calculate request duration
		requestDuration := time.Since(startTime)
		logMessage(LogLevelInfo, "[RequestID: %s] Keepa API response status code: %d, duration: %v", requestID, res.StatusCode, requestDuration)

		// If we got a 429, close the response body and retry
		if res.StatusCode == http.StatusTooManyRequests {
			res.Body.Close()
			if attempt == maxRetries {
				return nil, fmt.Errorf("still rate limited after %d retries", maxRetries+1)
			}
			continue
		}

		// For other non-200 status codes, handle them without retrying
		if res.StatusCode != http.StatusOK {
			defer res.Body.Close()
			var cause string
			switch res.StatusCode {
			case 400:
				cause = "REQUEST_REJECTED"
			case 402:
				cause = "PAYMENT_REQUIRED"
			case 405:
				cause = "METHOD_NOT_ALLOWED"
			default:
				cause = fmt.Sprintf("HTTP %d", res.StatusCode)
			}
			logMessage(LogLevelError, "[RequestID: %s] Failed to send request: %v", requestID, cause)
			return nil, fmt.Errorf("[RequestID: %s] Failed to send request: %v", requestID, cause)
		}

		// Read response body
		body, err = io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			if attempt == maxRetries {
				return nil, fmt.Errorf("failed to read response body after %d attempts: %v", maxRetries+1, err)
			}
			logMessage(LogLevelError, "[RequestID: %s] Failed to read response body (attempt %d): %v", requestID, attempt+1, err)
			continue
		}

		// If we got here, we have a successful response, so break out of the retry loop
		break
	}

	logMessage(LogLevelDebug, "[RequestID: %s] Response body size: %d bytes", requestID, len(body))

	// Parse the Keepa API response
	if err = json.Unmarshal(body, &keepaResponse); err != nil {
		logMessage(LogLevelError, "[RequestID: %s] Failed to parse Keepa API response: %v", requestID, err)
		return nil, fmt.Errorf("[RequestID: %s] Failed to parse Keepa API response: %v", requestID, err)
	}

	simplifiedResponse := &SimplifiedResponse{Products: make([]SimplifiedProduct, 0)}

	// Extract the needed fields from each product
	for _, product := range keepaResponse.Products {
		rootCategory := strconv.Itoa(product.RootCategory)

		// Create sales ranks map with timestamp as key and rank as value
		salesRanks := make(map[string]int)
		if len(product.SalesRanks[rootCategory]) > 0 && len(product.SalesRanks[rootCategory])%2 == 0 {
			for i := 0; i < len(product.SalesRanks[rootCategory]); i += 2 {
				timestamp := time.UnixMilli(int64(product.SalesRanks[rootCategory][i]+21564000) * 60000)
				timestampStr := timestamp.Format(time.DateTime)
				salesRanks[timestampStr] = product.SalesRanks[rootCategory][i+1]
			}
		}

		simplifiedProduct := SimplifiedProduct{
			Asin:       product.Asin,
			Title:      product.Title,
			Categories: product.Categories,
			Brand:      product.Brand,
			SalesRanks: salesRanks,
		}

		// Add buyBoxPrice if available
		if product.Stats.BuyBoxPrice != 0 {
			simplifiedProduct.BuyBoxPrice = product.Stats.BuyBoxPrice
		}

		// Add simplified offers
		for _, offer := range product.Offers {
			simplifiedOffer := SimplifiedOffer{
				SellerID:  offer.SellerID,
				Condition: offer.Condition,
				IsPrime:   offer.IsPrime,
				IsAmazon:  offer.IsAmazon,
				IsFBA:     offer.IsFBA,
			}

			// Only include stockCSV if it's not empty
			if len(offer.StockCSV) > 0 && len(offer.StockCSV)%2 == 0 {
				stockCSV := make(map[string]int)
				for i := 0; i < len(offer.StockCSV); i += 2 {
					timestamp := time.UnixMilli(int64(offer.StockCSV[i]+21564000) * 60000)
					timestampStr := timestamp.Format(time.DateTime)
					stockCSV[timestampStr] = offer.StockCSV[i+1]
				}
				simplifiedOffer.StockCSV = stockCSV
			}

			simplifiedProduct.Offers = append(simplifiedProduct.Offers, simplifiedOffer)
		}

		simplifiedResponse.Products = append(simplifiedResponse.Products, simplifiedProduct)
	}

	return simplifiedResponse, nil
}

// Get environment variable, return default value if not set
func getEnvWithDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
