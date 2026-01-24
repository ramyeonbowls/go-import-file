package model

import "time"

type SpProsesDpZdhdr struct {
	ProcessId           string
	BlockId             string
	BlockName           string
	ConditionType       string
	Keycombination      string
	Keycomb             string
	SalesOrganization   string
	DistributionChannel string
	SalesOffice         string
	Division            string
	PaymentTerm         string
	Customer            string
	Material            string
	Attribut2           string
	ValidUntil          time.Time
	ValidFrom           time.Time
	ConditionRecordno   string
	Scale               string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

type SpProsesDpZditm struct {
	ProcessId           string
	BlockId             string
	BlockName           string
	ConditionType       string
	KeyCombination      string
	KeyComb             string
	SalesOrganization   string
	DistributionChannel string
	SalesOffice         string
	Division            string
	SoldToParty         string
	PricingRefMatl      string
	PaymentTerms        string
	IndustryCode3       string
	IndustryCode4       string
	IndustryCode5       string
	Attribute1          string
	Attribute2          string
	Material            string
	SalesUnit           string
	ValidFrom           time.Time
	ValidUntil          time.Time
	ConditionRecordNo   string
	Scale               string
	FileName            string
	LineNumber          int
	CDate               time.Time
}

type SpProsesDpZddet struct {
	ProcessId         string
	BlockId           string
	BlockName         string
	ConditionRecordNo string
	Amount            float64
	Unit              string
	Per               float64
	Uom               string
	Scale             string
	Filename          string
	Linenumber        int
	Cdate             time.Time
}

type SpProsesDpZpmix struct {
	ProcessId      string
	BlockId        string
	Blockname      string
	Ctyp           string
	KeyCombination string
	Sorg           string
	Dchl           string
	Soff           string
	Dv             string
	Customer       string
	Indcode2       string
	Indcode3       string
	Indcode4       string
	Indcode5       string
	Pl             string
	Payt           string
	Material       string
	ValidFrom      time.Time
	ValidUntil     time.Time
	PromoId        string
	LineItem       int
	FileName       string
	LineNumber     int
	Cdate          time.Time
	MustBuy        string
	Exclude        string
	Split          string
	Amountx        string
	Rangex         string
	WithMaterial   string
	Kelipatan      string
	VKelipatan     int
	AttrPrdLv2     string
	AttrPrdLv3     string
	FlCustExc      string
	CustExc        string
	FlHd           string
	Perbandingan   string
	VPerbandingan1 int
	VPerbandingan2 int
}

type SpProsesDpZscreg struct {
	ProcessId         string
	BlockId           string
	BlockName         string
	ConditionRecordNo string
	No                int
	Lsno              int
	DiscRegHdrQty     float64
	Amount            float64
	Unit              string
	FileName          string
	LineNumber        int
	Cdate             time.Time
}

type SpProsesDpZscmix struct {
	ProcessId   string
	BlockId     string
	BlockName   string
	PromoId     string
	LineItem    int
	ScaleQty    float64
	Bun         string
	Amount      float64
	Unit        string
	Per         float64
	Uom         string
	FileName    string
	LineNumber  int
	Cdate       time.Time
	ScaleQtyTo  float64
	AmountScl   float64
	AmountSclTo float64
	UnitScl     string
	MatnrKena   string
}

type SpProsesDpZ00001 struct {
	ProcessId     string
	BlockId       string
	BlockName     string
	Step          string
	Counter       string
	ConditionType string
	Description   string
	ValidFrom     int
	ValidTo       int
	CondGrp       string
	Drule         string
	FileName      string
	LineNumber    int
	Cdate         time.Time
	DiscType      string
}

type SpProsesFgZdhdr struct {
	ProcessId           string
	BlockId             string
	BlockName           string
	ConditionType       string
	KeyCombination      string
	KeyComb             string
	SalesOrganization   string
	DistributionChannel string
	Division            string
	SalesOffice         string
	PricelistType       string
	Attribute1          string
	IndustryCode3       string
	IndustryCode4       string
	IndustryCode5       string
	SoldToParty         string
	Material            string
	ValidUntil          time.Time
	ValidFrom           time.Time
	ConditionRecordNo   string
	PromoId             string
	PromoItem           string
	Scale               string
	FileName            string
	LineNumber          int
	CDate               time.Time
	MustBuy             string
	Kelipatan           string
	FKelipatan          int
	WithQty             string
	Qty                 int
	Uom                 float64
	Zterm               string
	Katr2               string
	Katr3               string
	Perbandingan        string
	FPerbandingan1      int
	FPerbandingan2      int
	Amountx             string
}

type SpProsesFgZfrdet struct {
	ProcessId          string
	BlockId            string
	BlockName          string
	ConditionRecordNo  string
	MinimumQty         float64
	FreeGoodsQty       float64
	UomFreeGoods       string
	FreeGoodsAgrredQty float64
	UomFreeGoodsAgrred string
	AdditionalMaterial string
	FileName           string
	LineNumber         int
	CDate              time.Time
}

type SpProsesFgZfrmix struct {
	ProcessId   string
	BlockId     string
	BlockName   string
	PromoId     string
	PromoItem   string
	ScaleQty    float64
	ScaleQtyUom string
	Material    string
	Qty         float64
	QtyUom      string
	FileName    string
	LineNumber  int
	CDate       time.Time
	AmountSclf  float64
	Currency    string
}
