package main

import (
	"database/sql"
	"embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed templates/*
var templateFS embed.FS

var db *sql.DB
var tmpl *template.Template
var endDate string

type Property struct {
	DP              string
	Address         string
	City            string
	State           string
	Zip             string
	Class           string
	ClassDescr      string
	LandFull        int
	BldgFull        int
	TotalFull       int
	LandAcres       float64
	TotalLivingArea int
	Bedrooms        int
	Bathrooms       int
	YearBuilt       int
	Condition       string
	Grade           string
	SchoolDistrict  string
	TitleHolder1    string
	TitleHolder2    string
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

type PageData struct {
	Query    string
	Property *Property
	Sales    []Sale
	Error    string
	EndDate  string
}

func main() {
	addr := flag.String("addr", "0.0.0.0:8080", "bind address (e.g., localhost:8080)")
	dbPath := flag.String("db", "polk_county.db", "path to SQLite database")
	dataEndDate := flag.String("end-date", "2026", "end date for data range shown in footer")
	flag.Parse()
	endDate = *dataEndDate

	var err error
	db, err = sql.Open("sqlite3", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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
	}).ParseFS(templateFS, "templates/*.html"))

	http.HandleFunc("/", handleSearch)
	log.Printf("Server running on http://%s (db: %s)\n", *addr, *dbPath)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	data := PageData{EndDate: endDate}

	query := strings.TrimSpace(r.URL.Query().Get("q"))
	data.Query = query

	// Log request
	ip := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		ip = fwd
	}
	log.Printf("REQUEST ip=%s query=%q", ip, query)

	if query != "" {
		prop, err := findProperty(query)
		if err != nil {
			data.Error = "Property not found"
			log.Printf("ERROR finding property: %v", err)
		} else {
			data.Property = prop
			sales, err := findSales(prop.DP)
			if err != nil {
				log.Printf("ERROR finding sales for dp=%s: %v", prop.DP, err)
			}
			data.Sales = sales
		}
	}

	tmpl.ExecuteTemplate(w, "index.html", data)
}

func findProperty(query string) (*Property, error) {
	q := "%" + strings.ToUpper(query) + "%"

	row := db.QueryRow(`
		SELECT dp, address_line1, city, state, zip, class, class_descr,
			   COALESCE(land_full, 0), COALESCE(bldg_full, 0), COALESCE(total_full, 0),
			   COALESCE(land_acres, 0), COALESCE(total_living_area, 0),
			   COALESCE(bedrooms, 0), COALESCE(bathrooms, 0), COALESCE(year_built, 0),
			   COALESCE(condition, ''), COALESCE(grade, ''), COALESCE(school_district, ''),
			   COALESCE(title_holder1, ''), COALESCE(title_holder2, '')
		FROM properties
		WHERE dp LIKE ? OR address_line1 LIKE ? OR title_holder1 LIKE ?
		LIMIT 1
	`, q, q, q)

	var p Property
	err := row.Scan(&p.DP, &p.Address, &p.City, &p.State, &p.Zip, &p.Class, &p.ClassDescr,
		&p.LandFull, &p.BldgFull, &p.TotalFull, &p.LandAcres, &p.TotalLivingArea,
		&p.Bedrooms, &p.Bathrooms, &p.YearBuilt, &p.Condition, &p.Grade, &p.SchoolDistrict,
		&p.TitleHolder1, &p.TitleHolder2)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func findSales(dp string) ([]Sale, error) {
	rows, err := db.Query(`
		SELECT COALESCE(sale_date, ''), COALESCE(price, 0), COALESCE(book, ''), COALESCE(pg, ''),
			   COALESCE(seller, ''), COALESCE(buyer, ''), COALESCE(quality1, ''), COALESCE(analysis_quality, '')
		FROM sales
		WHERE dp = ?
		ORDER BY sale_date DESC
	`, dp)
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
