package main

import (
	"database/sql"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//go:embed templates/*
var templateFS embed.FS

var db *sql.DB
var searchLogDB *sql.DB
var tmpl *template.Template
var endDate string

// Prometheus metrics
var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "polksearch_requests_total",
			Help: "Total number of requests",
		},
		[]string{"type"}, // "search", "view", "home"
	)
	searchResultsHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "polksearch_search_results",
			Help:    "Number of results returned by searches",
			Buckets: []float64{0, 1, 10, 50, 100, 500, 1000, 5000},
		},
	)
)

func init() {
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(searchResultsHist)
}

type Property struct {
	// Core identification
	DP   string
	GP   string
	Nbhd string

	// Address
	Address  string
	City     string
	State    string
	Zip      string
	Zip4     string

	// Classification
	Class      string
	ClassDescr string

	// Valuation
	LandFull  int
	BldgFull  int
	AgbdFull  int // Agricultural building value
	TotalFull int
	LandAdj   int
	BldgAdj   int
	AgbdAdj   int
	TotalAdj  int

	// Land
	LandAcres float64
	LandSF    int // residential
	LandArea  int // commercial
	Frontage  float64
	Depth     float64

	// Ownership
	TitleHolder1 string
	TitleHolder2 string
	Transfer1    string
	Book1        string
	Pg1          string
	Transfer2    string
	Book2        string
	Pg2          string

	// Contract buyers
	ContractBuyer1 string
	ContractBuyer2 string

	// Mailing address
	MailName    string
	MailLine1   string
	MailLine2   string
	MailCity    string
	MailState   string
	MailZip     string

	// Building basics
	YearBuilt    int
	YearRemodel  int
	EffYearBuilt int
	Condition    string
	Grade        string

	// Legal/Location
	SchoolDistrict string
	Platname       string
	Legal          string
	TIF            int
	TIFDescr       string

	// Coordinates
	X float64
	Y float64

	// === RESIDENTIAL-SPECIFIC ===
	ResidenceType    string
	BldgStyle        string
	ExteriorWallType string
	RoofType         string
	RoofMaterial     string
	Foundation       string
	Heating          string
	AirConditioning  int
	PercentBrick     int

	// Living areas
	MainLivingArea  int
	UpperLivingArea int
	TotalLivingArea int
	FinAtticArea    int
	UnfinAtticArea  int
	CondoFinLivArea int
	CondoYearBuilt  int

	// Basement
	BasementArea    int
	FinBsmtAreaTot  int
	FinBsmtArea1    int
	FinBsmtArea2    int
	FinBsmtQual1    string
	FinBsmtQual2    string
	BsmtWalkout     int
	BsmtGarCapacity int

	// Garage/Parking
	AttGarageArea int
	GarageBrick   int
	CarportArea   int

	// Outdoor
	OpenPorchArea    int
	EnclosePorchArea int
	PatioArea        int
	DeckArea         int
	CanopyArea       int
	VeneerArea       int

	// Rooms
	Bedrooms      int
	Bathrooms     int
	ToiletRooms   int
	Rooms         int
	Families      int
	ExtraFixtures int

	// Amenities
	Fireplaces int
	Whirlpools int
	Hottubs    int
	Saunas     int

	// Other residential
	DetachedStructs    string
	CommercialOccupancy string
	CommercialArea     int
	PercentComplete    int

	// === COMMERCIAL-SPECIFIC ===
	OccupancyGroup      string
	OccupancyGroupDescr string
	Occupancy           string
	OccupancyDescr      string
	PrimaryGroup        string
	SecondaryGroup      string
	PercentPrimary      int
	PercentSecondary    int
	BldgClass           string
	Occupant            string
	Zoning              string
	NumberUnits         int

	// Commercial building areas
	GrossArea              int
	TotalStoryHeight       int
	GroundFloorArea        int
	Perimeter              int
	WallHeight             int
	FinishedArea           int
	UnfinishedArea         int
	MezzanineFinishedArea  int
	MezzanineUnfinishedArea int
	AirCondArea            int
	SprinkleArea           int

	// Commercial basement
	BsmtUnfinishedArea int
	BsmtFinishedArea   int
	BsmtParkingArea    int

	// Elevators
	NumPassengerStops int
	NumFreightStops   int
}

type Sale struct {
	SaleDate        string
	Price           int
	Book            string
	Pg              string
	Seller          string
	Buyer           string
	Quality1        string
	AnalysisQuality string
}

type SearchResult struct {
	DP           string
	Address      string
	City         string
	TitleHolder1 string
	TotalFull    int
}

type PageData struct {
	Query          string
	PropType       string // "res", "com", or "agr"
	IsCommercial   bool
	IsAgricultural bool
	Property       *Property
	Results        []SearchResult
	Sales          []Sale
	Error          string
	EndDate        string
	Page           int
	TotalPages     int
	TotalCount     int
	HasPrev        bool
	HasNext        bool
}

func main() {
	addr := flag.String("addr", "0.0.0.0:8080", "bind address (e.g., localhost:8080)")
	metricsAddr := flag.String("metrics-addr", ":9090", "metrics server bind address")
	dbPath := flag.String("db", "polk_county.db", "path to SQLite database")
	dataEndDate := flag.String("end-date", "2026", "end date for data range shown in footer")
	searchLogPath := flag.String("log-searches", "", "path to SQLite database for logging search terms (enables search logging)")
	flag.Parse()
	endDate = *dataEndDate

	var err error
	db, err = sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create search log database if path specified
	if *searchLogPath != "" {
		searchLogDB, err = sql.Open("sqlite3", *searchLogPath)
		if err != nil {
			log.Fatalf("ERROR opening search log database: %v", err)
		}
		defer searchLogDB.Close()

		_, err = searchLogDB.Exec(`
			CREATE TABLE IF NOT EXISTS search_log (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				timestamp TEXT,
				ip TEXT,
				query TEXT,
				result_count INTEGER
			)
		`)
		if err != nil {
			log.Fatalf("ERROR creating search_log table: %v", err)
		}
		log.Printf("Search logging enabled (db: %s)", *searchLogPath)
	}

	tmpl = template.Must(template.New("").Funcs(template.FuncMap{
		"formatPrice": func(p int) string {
			if p == 0 {
				return "-"
			}
			s := ""
			ps := fmt.Sprintf("%d", p)
			for i, c := range ps {
				if i > 0 && (len(ps)-i)%3 == 0 {
					s += ","
				}
				s += string(c)
			}
			return "$" + s
		},
		"add":      func(a, b int) int { return a + b },
		"subtract": func(a, b int) int { return a - b },
	}).ParseFS(templateFS, "templates/*.html"))

	// Start metrics server on separate port
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics server running on http://%s/metrics\n", *metricsAddr)
		log.Fatal(http.ListenAndServe(*metricsAddr, mux))
	}()

	http.HandleFunc("/", handleSearch)
	log.Printf("Server running on http://%s (db: %s)\n", *addr, *dbPath)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

const pageSize = 50

func handleSearch(w http.ResponseWriter, r *http.Request) {
	data := PageData{EndDate: endDate, Page: 1, PropType: "res"}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	dp := strings.TrimSpace(r.URL.Query().Get("dp"))
	pageStr := r.URL.Query().Get("page")
	propType := r.URL.Query().Get("type")

	data.Query = query
	if propType == "com" {
		data.PropType = "com"
		data.IsCommercial = true
	} else if propType == "agr" {
		data.PropType = "agr"
		data.IsAgricultural = true
	}

	if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
		data.Page = p
	}

	// Get IP
	ip := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		ip = fwd
	}
	log.Printf("REQUEST ip=%s query=%q dp=%q type=%s page=%d", ip, query, dp, data.PropType, data.Page)

	// If dp is specified, show that specific property
	if dp != "" {
		requestsTotal.WithLabelValues("view").Inc()
		prop, err := getPropertyByDP(dp, data.PropType)
		if err != nil {
			data.Error = "Property not found"
			log.Printf("ERROR finding property by dp: %v", err)
		} else {
			data.Property = prop
			sales, err := findSales(prop.DP, data.PropType)
			if err != nil {
				log.Printf("ERROR finding sales for dp=%s: %v", prop.DP, err)
			}
			data.Sales = sales
		}
	} else if query != "" {
		requestsTotal.WithLabelValues("search").Inc()

		// Get total count first
		count, err := countProperties(query, data.PropType)
		if err != nil {
			data.Error = "Search failed"
			log.Printf("ERROR counting properties: %v", err)
		} else {
			// Record metrics
			searchResultsHist.Observe(float64(count))

			// Log search to database if enabled
			if searchLogDB != nil {
				go logSearch(ip, query, count)
			}

			if count == 0 {
				data.Error = "No properties found"
			} else if count == 1 {
				// Single result - show detail directly
				results, _ := searchProperties(query, 1, 0, data.PropType)
				if len(results) > 0 {
					prop, err := getPropertyByDP(results[0].DP, data.PropType)
					if err != nil {
						data.Error = "Property not found"
						log.Printf("ERROR finding property: %v", err)
					} else {
						data.Property = prop
						sales, err := findSales(prop.DP, data.PropType)
						if err != nil {
							log.Printf("ERROR finding sales for dp=%s: %v", prop.DP, err)
						}
						data.Sales = sales
					}
				}
			} else {
				// Multiple results - show paginated list
				data.TotalCount = count
				data.TotalPages = (count + pageSize - 1) / pageSize
				if data.Page > data.TotalPages {
					data.Page = data.TotalPages
				}
				offset := (data.Page - 1) * pageSize
				data.HasPrev = data.Page > 1
				data.HasNext = data.Page < data.TotalPages

				results, err := searchProperties(query, pageSize, offset, data.PropType)
				if err != nil {
					data.Error = "Search failed"
					log.Printf("ERROR searching properties: %v", err)
				} else {
					data.Results = results
				}
			}
		}
	} else {
		requestsTotal.WithLabelValues("home").Inc()
	}

	tmpl.ExecuteTemplate(w, "index.html", data)
}

func logSearch(ip, query string, resultCount int) {
	_, err := searchLogDB.Exec(`
		INSERT INTO search_log (timestamp, ip, query, result_count)
		VALUES (?, ?, ?, ?)
	`, time.Now().UTC().Format(time.RFC3339), ip, query, resultCount)
	if err != nil {
		log.Printf("ERROR logging search: %v", err)
	}
}

func countProperties(query string, propType string) (int, error) {
	q := "%" + strings.ToUpper(query) + "%"

	var count int
	var err error
	switch propType {
	case "com":
		err = db.QueryRow(`
			SELECT COUNT(*)
			FROM commercial_properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
			   OR occupancy_group LIKE ? OR occupancy_group_descr LIKE ?
		`, q, q, q, q, q, q).Scan(&count)
	case "agr":
		err = db.QueryRow(`
			SELECT COUNT(*)
			FROM agricultural_properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
		`, q, q, q, q).Scan(&count)
	default:
		err = db.QueryRow(`
			SELECT COUNT(*)
			FROM properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
		`, q, q, q, q).Scan(&count)
	}
	return count, err
}

func searchProperties(query string, limit, offset int, propType string) ([]SearchResult, error) {
	q := "%" + strings.ToUpper(query) + "%"

	var rows *sql.Rows
	var err error
	switch propType {
	case "com":
		rows, err = db.Query(`
			SELECT dp, COALESCE(address_line1, ''), COALESCE(city, ''),
			       COALESCE(title_holder1, ''), COALESCE(total_full, 0)
			FROM commercial_properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
			   OR occupancy_group LIKE ? OR occupancy_group_descr LIKE ?
			ORDER BY address_line1
			LIMIT ? OFFSET ?
		`, q, q, q, q, q, q, limit, offset)
	case "agr":
		rows, err = db.Query(`
			SELECT dp, COALESCE(address_line1, ''), COALESCE(city, ''),
			       COALESCE(title_holder1, ''), COALESCE(total_full, 0)
			FROM agricultural_properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
			ORDER BY address_line1
			LIMIT ? OFFSET ?
		`, q, q, q, q, limit, offset)
	default:
		rows, err = db.Query(`
			SELECT dp, COALESCE(address_line1, ''), COALESCE(city, ''),
			       COALESCE(title_holder1, ''), COALESCE(total_full, 0)
			FROM properties
			WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ? OR title_holder2 LIKE ?
			ORDER BY address_line1
			LIMIT ? OFFSET ?
		`, q, q, q, q, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		rows.Scan(&r.DP, &r.Address, &r.City, &r.TitleHolder1, &r.TotalFull)
		results = append(results, r)
	}
	return results, nil
}

func getPropertyByDP(dp string, propType string) (*Property, error) {
	var p Property

	if propType == "com" {
		row := db.QueryRow(`
			SELECT
				COALESCE(dp, ''), COALESCE(gp, ''), COALESCE(nbhd, ''),
				COALESCE(address_line1, ''), COALESCE(city, ''), COALESCE(state, ''), COALESCE(zip, ''), COALESCE(zip4, ''),
				COALESCE(class, ''), COALESCE(class_descr, ''),
				COALESCE(land_full, 0), COALESCE(bldg_full, 0), COALESCE(total_full, 0),
				COALESCE(land_adj, 0), COALESCE(bldg_adj, 0), COALESCE(total_adj, 0),
				COALESCE(land_area, 0) / 43560.0, COALESCE(land_area, 0),
				COALESCE(frontage, 0), COALESCE(depth, 0),
				COALESCE(title_holder1, ''), COALESCE(title_holder2, ''),
				COALESCE(transfer_th1, ''), COALESCE(book_th1, ''), COALESCE(pg_th1, ''),
				COALESCE(transfer_cb1, ''), COALESCE(book_cb1, ''), COALESCE(pg_cb1, ''),
				COALESCE(contract_buyer1, ''), COALESCE(contract_buyer2, ''),
				COALESCE(mail_name, ''), COALESCE(mail_line1, ''), COALESCE(mail_line2, ''),
				COALESCE(mail_city, ''), COALESCE(mail_state, ''), COALESCE(mail_zip, ''),
				COALESCE(year_built, 0), COALESCE(condition, ''), COALESCE(grade, ''),
				COALESCE(school_district, ''), COALESCE(platname, ''), COALESCE(legal, ''),
				COALESCE(tif, 0), COALESCE(tif_descr, ''),
				COALESCE(x, 0), COALESCE(y, 0),
				COALESCE(occupancy_group, ''), COALESCE(occupancy_group_descr, ''),
				COALESCE(occupancy, ''), COALESCE(occupancy_descr, ''),
				COALESCE(primary_group, ''), COALESCE(secondary_group, ''),
				COALESCE(percent_primary, 0), COALESCE(percent_secondary, 0),
				COALESCE(bldg_class, ''), COALESCE(occupant, ''), COALESCE(zoning, ''),
				COALESCE(number_units, 0),
				COALESCE(gross_area, 0), COALESCE(total_story_height, 0), COALESCE(ground_floor_area, 0),
				COALESCE(perimeter, 0), COALESCE(wall_height, 0),
				COALESCE(finished_area, 0), COALESCE(unfinished_area, 0),
				COALESCE(mezzanine_finished_area, 0), COALESCE(mezzanine_unfinished_area, 0),
				COALESCE(air_cond_area, 0), COALESCE(sprinkle_area, 0),
				COALESCE(bsmt_unfinished_area, 0), COALESCE(bsmt_finished_area, 0), COALESCE(bsmt_parking_area, 0),
				COALESCE(number_passenger_stops, 0), COALESCE(number_freight_stops, 0)
			FROM commercial_properties
			WHERE dp = ?
		`, dp)
		err := row.Scan(
			&p.DP, &p.GP, &p.Nbhd,
			&p.Address, &p.City, &p.State, &p.Zip, &p.Zip4,
			&p.Class, &p.ClassDescr,
			&p.LandFull, &p.BldgFull, &p.TotalFull,
			&p.LandAdj, &p.BldgAdj, &p.TotalAdj,
			&p.LandAcres, &p.LandArea,
			&p.Frontage, &p.Depth,
			&p.TitleHolder1, &p.TitleHolder2,
			&p.Transfer1, &p.Book1, &p.Pg1,
			&p.Transfer2, &p.Book2, &p.Pg2,
			&p.ContractBuyer1, &p.ContractBuyer2,
			&p.MailName, &p.MailLine1, &p.MailLine2,
			&p.MailCity, &p.MailState, &p.MailZip,
			&p.YearBuilt, &p.Condition, &p.Grade,
			&p.SchoolDistrict, &p.Platname, &p.Legal,
			&p.TIF, &p.TIFDescr,
			&p.X, &p.Y,
			&p.OccupancyGroup, &p.OccupancyGroupDescr,
			&p.Occupancy, &p.OccupancyDescr,
			&p.PrimaryGroup, &p.SecondaryGroup,
			&p.PercentPrimary, &p.PercentSecondary,
			&p.BldgClass, &p.Occupant, &p.Zoning,
			&p.NumberUnits,
			&p.GrossArea, &p.TotalStoryHeight, &p.GroundFloorArea,
			&p.Perimeter, &p.WallHeight,
			&p.FinishedArea, &p.UnfinishedArea,
			&p.MezzanineFinishedArea, &p.MezzanineUnfinishedArea,
			&p.AirCondArea, &p.SprinkleArea,
			&p.BsmtUnfinishedArea, &p.BsmtFinishedArea, &p.BsmtParkingArea,
			&p.NumPassengerStops, &p.NumFreightStops,
		)
		if err != nil {
			return nil, err
		}
	} else if propType == "agr" {
		row := db.QueryRow(`
			SELECT
				COALESCE(dp, ''), COALESCE(gp, ''), COALESCE(nbhd, ''),
				COALESCE(address_line1, ''), COALESCE(city, ''), COALESCE(state, ''), COALESCE(zip, ''), COALESCE(zip4, ''),
				COALESCE(class, ''), COALESCE(class_descr, ''),
				COALESCE(land_full, 0), COALESCE(bldg_full, 0), COALESCE(agbd_full, 0), COALESCE(total_full, 0),
				COALESCE(land_adj, 0), COALESCE(bldg_adj, 0), COALESCE(agbd_adj, 0), COALESCE(total_adj, 0),
				COALESCE(land_acres, 0), COALESCE(land_sf, 0),
				COALESCE(frontage, 0), COALESCE(depth, 0),
				COALESCE(title_holder1, ''), COALESCE(title_holder2, ''),
				COALESCE(transfer_th1, ''), COALESCE(book_th1, ''), COALESCE(pg_th1, ''),
				COALESCE(transfer_cb1, ''), COALESCE(book_cb1, ''), COALESCE(pg_cb1, ''),
				COALESCE(contract_buyer1, ''), COALESCE(contract_buyer2, ''),
				COALESCE(mail_name, ''), COALESCE(mail_line1, ''), COALESCE(mail_line2, ''),
				COALESCE(mail_city, ''), COALESCE(mail_state, ''), COALESCE(mail_zip, ''),
				COALESCE(year_built, 0), COALESCE(year_remodel, 0), COALESCE(eff_year_built, 0),
				COALESCE(condition, ''), COALESCE(grade, ''),
				COALESCE(school_district, ''), COALESCE(platname, ''), COALESCE(legal, ''),
				COALESCE(tif, 0), COALESCE(tif_descr, ''),
				COALESCE(x, 0), COALESCE(y, 0),
				COALESCE(residence_type, ''), COALESCE(bldg_style, ''),
				COALESCE(exterior_wall_type, ''), COALESCE(roof_type, ''), COALESCE(roof_material, ''),
				COALESCE(foundation, ''), COALESCE(heating, ''),
				COALESCE(air_conditioning, 0), COALESCE(percent_brick, 0),
				COALESCE(main_living_area, 0), COALESCE(upper_living_area, 0), COALESCE(total_living_area, 0),
				COALESCE(fin_attic_area, 0), COALESCE(unfin_attic_area, 0),
				COALESCE(condo_fin_liv_area, 0), COALESCE(condo_year_built, 0),
				COALESCE(basement_area, 0), COALESCE(fin_bsmt_area_tot, 0),
				COALESCE(fin_bsmt_area1, 0), COALESCE(fin_bsmt_area2, 0),
				COALESCE(fin_bsmt_qual1, ''), COALESCE(fin_bsmt_qual2, ''),
				COALESCE(bsmt_walkout, 0), COALESCE(bsmt_gar_capacity, 0),
				COALESCE(att_garage_area, 0), COALESCE(garage_brick, 0), COALESCE(carport_area, 0),
				COALESCE(open_porch_area, 0), COALESCE(enclose_porch_area, 0),
				COALESCE(patio_area, 0), COALESCE(deck_area, 0),
				COALESCE(canopy_area, 0), COALESCE(veneer_area, 0),
				COALESCE(bedrooms, 0), COALESCE(bathrooms, 0), COALESCE(toilet_rooms, 0),
				COALESCE(rooms, 0), COALESCE(families, 0), COALESCE(extra_fixtures, 0),
				COALESCE(fireplaces, 0), COALESCE(whirlpools, 0), COALESCE(hottubs, 0), COALESCE(saunas, 0),
				COALESCE(detached_structs, ''), COALESCE(commercial_occupancy, ''), COALESCE(commercial_area, 0),
				COALESCE(percent_complete, 0)
			FROM agricultural_properties
			WHERE dp = ?
		`, dp)
		err := row.Scan(
			&p.DP, &p.GP, &p.Nbhd,
			&p.Address, &p.City, &p.State, &p.Zip, &p.Zip4,
			&p.Class, &p.ClassDescr,
			&p.LandFull, &p.BldgFull, &p.AgbdFull, &p.TotalFull,
			&p.LandAdj, &p.BldgAdj, &p.AgbdAdj, &p.TotalAdj,
			&p.LandAcres, &p.LandSF,
			&p.Frontage, &p.Depth,
			&p.TitleHolder1, &p.TitleHolder2,
			&p.Transfer1, &p.Book1, &p.Pg1,
			&p.Transfer2, &p.Book2, &p.Pg2,
			&p.ContractBuyer1, &p.ContractBuyer2,
			&p.MailName, &p.MailLine1, &p.MailLine2,
			&p.MailCity, &p.MailState, &p.MailZip,
			&p.YearBuilt, &p.YearRemodel, &p.EffYearBuilt,
			&p.Condition, &p.Grade,
			&p.SchoolDistrict, &p.Platname, &p.Legal,
			&p.TIF, &p.TIFDescr,
			&p.X, &p.Y,
			&p.ResidenceType, &p.BldgStyle,
			&p.ExteriorWallType, &p.RoofType, &p.RoofMaterial,
			&p.Foundation, &p.Heating,
			&p.AirConditioning, &p.PercentBrick,
			&p.MainLivingArea, &p.UpperLivingArea, &p.TotalLivingArea,
			&p.FinAtticArea, &p.UnfinAtticArea,
			&p.CondoFinLivArea, &p.CondoYearBuilt,
			&p.BasementArea, &p.FinBsmtAreaTot,
			&p.FinBsmtArea1, &p.FinBsmtArea2,
			&p.FinBsmtQual1, &p.FinBsmtQual2,
			&p.BsmtWalkout, &p.BsmtGarCapacity,
			&p.AttGarageArea, &p.GarageBrick, &p.CarportArea,
			&p.OpenPorchArea, &p.EnclosePorchArea,
			&p.PatioArea, &p.DeckArea,
			&p.CanopyArea, &p.VeneerArea,
			&p.Bedrooms, &p.Bathrooms, &p.ToiletRooms,
			&p.Rooms, &p.Families, &p.ExtraFixtures,
			&p.Fireplaces, &p.Whirlpools, &p.Hottubs, &p.Saunas,
			&p.DetachedStructs, &p.CommercialOccupancy, &p.CommercialArea,
			&p.PercentComplete,
		)
		if err != nil {
			return nil, err
		}
	} else {
		row := db.QueryRow(`
			SELECT
				COALESCE(dp, ''), COALESCE(gp, ''), COALESCE(nbhd, ''),
				COALESCE(address_line1, ''), COALESCE(city, ''), COALESCE(state, ''), COALESCE(zip, ''), COALESCE(zip4, ''),
				COALESCE(class, ''), COALESCE(class_descr, ''),
				COALESCE(land_full, 0), COALESCE(bldg_full, 0), COALESCE(total_full, 0),
				COALESCE(land_adj, 0), COALESCE(bldg_adj, 0), COALESCE(total_adj, 0),
				COALESCE(land_acres, 0), COALESCE(land_sf, 0),
				COALESCE(frontage, 0), COALESCE(depth, 0),
				COALESCE(title_holder1, ''), COALESCE(title_holder2, ''),
				COALESCE(transfer_th1, ''), COALESCE(book_th1, ''), COALESCE(pg_th1, ''),
				COALESCE(transfer_cb1, ''), COALESCE(book_cb1, ''), COALESCE(pg_cb1, ''),
				COALESCE(contract_buyer1, ''), COALESCE(contract_buyer2, ''),
				COALESCE(mail_name, ''), COALESCE(mail_line1, ''), COALESCE(mail_line2, ''),
				COALESCE(mail_city, ''), COALESCE(mail_state, ''), COALESCE(mail_zip, ''),
				COALESCE(year_built, 0), COALESCE(year_remodel, 0), COALESCE(eff_year_built, 0),
				COALESCE(condition, ''), COALESCE(grade, ''),
				COALESCE(school_district, ''), COALESCE(platname, ''), COALESCE(legal, ''),
				COALESCE(tif, 0), COALESCE(tif_descr, ''),
				COALESCE(x, 0), COALESCE(y, 0),
				COALESCE(residence_type, ''), COALESCE(bldg_style, ''),
				COALESCE(exterior_wall_type, ''), COALESCE(roof_type, ''), COALESCE(roof_material, ''),
				COALESCE(foundation, ''), COALESCE(heating, ''),
				COALESCE(air_conditioning, 0), COALESCE(percent_brick, 0),
				COALESCE(main_living_area, 0), COALESCE(upper_living_area, 0), COALESCE(total_living_area, 0),
				COALESCE(fin_attic_area, 0), COALESCE(unfin_attic_area, 0),
				COALESCE(condo_fin_liv_area, 0), COALESCE(condo_year_built, 0),
				COALESCE(basement_area, 0), COALESCE(fin_bsmt_area_tot, 0),
				COALESCE(fin_bsmt_area1, 0), COALESCE(fin_bsmt_area2, 0),
				COALESCE(fin_bsmt_qual1, ''), COALESCE(fin_bsmt_qual2, ''),
				COALESCE(bsmt_walkout, 0), COALESCE(bsmt_gar_capacity, 0),
				COALESCE(att_garage_area, 0), COALESCE(garage_brick, 0), COALESCE(carport_area, 0),
				COALESCE(open_porch_area, 0), COALESCE(enclose_porch_area, 0),
				COALESCE(patio_area, 0), COALESCE(deck_area, 0),
				COALESCE(canopy_area, 0), COALESCE(veneer_area, 0),
				COALESCE(bedrooms, 0), COALESCE(bathrooms, 0), COALESCE(toilet_rooms, 0),
				COALESCE(rooms, 0), COALESCE(families, 0), COALESCE(extra_fixtures, 0),
				COALESCE(fireplaces, 0), COALESCE(whirlpools, 0), COALESCE(hottubs, 0), COALESCE(saunas, 0),
				COALESCE(detached_structs, ''), COALESCE(commercial_occupancy, ''), COALESCE(commercial_area, 0),
				COALESCE(percent_complete, 0)
			FROM properties
			WHERE dp = ?
		`, dp)
		err := row.Scan(
			&p.DP, &p.GP, &p.Nbhd,
			&p.Address, &p.City, &p.State, &p.Zip, &p.Zip4,
			&p.Class, &p.ClassDescr,
			&p.LandFull, &p.BldgFull, &p.TotalFull,
			&p.LandAdj, &p.BldgAdj, &p.TotalAdj,
			&p.LandAcres, &p.LandSF,
			&p.Frontage, &p.Depth,
			&p.TitleHolder1, &p.TitleHolder2,
			&p.Transfer1, &p.Book1, &p.Pg1,
			&p.Transfer2, &p.Book2, &p.Pg2,
			&p.ContractBuyer1, &p.ContractBuyer2,
			&p.MailName, &p.MailLine1, &p.MailLine2,
			&p.MailCity, &p.MailState, &p.MailZip,
			&p.YearBuilt, &p.YearRemodel, &p.EffYearBuilt,
			&p.Condition, &p.Grade,
			&p.SchoolDistrict, &p.Platname, &p.Legal,
			&p.TIF, &p.TIFDescr,
			&p.X, &p.Y,
			&p.ResidenceType, &p.BldgStyle,
			&p.ExteriorWallType, &p.RoofType, &p.RoofMaterial,
			&p.Foundation, &p.Heating,
			&p.AirConditioning, &p.PercentBrick,
			&p.MainLivingArea, &p.UpperLivingArea, &p.TotalLivingArea,
			&p.FinAtticArea, &p.UnfinAtticArea,
			&p.CondoFinLivArea, &p.CondoYearBuilt,
			&p.BasementArea, &p.FinBsmtAreaTot,
			&p.FinBsmtArea1, &p.FinBsmtArea2,
			&p.FinBsmtQual1, &p.FinBsmtQual2,
			&p.BsmtWalkout, &p.BsmtGarCapacity,
			&p.AttGarageArea, &p.GarageBrick, &p.CarportArea,
			&p.OpenPorchArea, &p.EnclosePorchArea,
			&p.PatioArea, &p.DeckArea,
			&p.CanopyArea, &p.VeneerArea,
			&p.Bedrooms, &p.Bathrooms, &p.ToiletRooms,
			&p.Rooms, &p.Families, &p.ExtraFixtures,
			&p.Fireplaces, &p.Whirlpools, &p.Hottubs, &p.Saunas,
			&p.DetachedStructs, &p.CommercialOccupancy, &p.CommercialArea,
			&p.PercentComplete,
		)
		if err != nil {
			return nil, err
		}
	}
	return &p, nil
}

func findSales(dp string, propType string) ([]Sale, error) {
	var rows *sql.Rows
	var err error

	switch propType {
	case "com":
		rows, err = db.Query(`
			SELECT COALESCE(sale_date, ''), COALESCE(price, 0), COALESCE(book, ''), COALESCE(pg, ''),
				   COALESCE(seller, ''), COALESCE(buyer, ''), COALESCE(quality, ''), ''
			FROM commercial_sales
			WHERE dp = ?
			ORDER BY sale_date DESC
		`, dp)
	case "agr":
		rows, err = db.Query(`
			SELECT COALESCE(sale_date, ''), COALESCE(price, 0), COALESCE(book, ''), COALESCE(pg, ''),
				   COALESCE(seller, ''), COALESCE(buyer, ''), COALESCE(quality1, ''), COALESCE(analysis_quality, '')
			FROM agricultural_sales
			WHERE dp = ?
			ORDER BY sale_date DESC
		`, dp)
	default:
		rows, err = db.Query(`
			SELECT COALESCE(sale_date, ''), COALESCE(price, 0), COALESCE(book, ''), COALESCE(pg, ''),
				   COALESCE(seller, ''), COALESCE(buyer, ''), COALESCE(quality1, ''), COALESCE(analysis_quality, '')
			FROM sales
			WHERE dp = ?
			ORDER BY sale_date DESC
		`, dp)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sales []Sale
	for rows.Next() {
		var s Sale
		rows.Scan(&s.SaleDate, &s.Price, &s.Book, &s.Pg, &s.Seller, &s.Buyer, &s.Quality1, &s.AnalysisQuality)
		sales = append(sales, s)
	}
	return sales, nil
}
