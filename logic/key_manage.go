package logic

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"sort"
	"strings"
	"xiepaup.com/keylifecycle/lib/db"
	comm "xiepaup.com/keylifecycle/logic/comm"
	//"sync"
	"time"
)

var CUR_AFTER_10MINS_TIME int64

func init() {
	CUR_AFTER_10MINS_TIME = time.Now().Unix() * 1000
}

type GuessKeyMode struct {
	StatFromFile       bool
	MaxGusessCnt       int64
	SplitDelimiter     string
	TrueSplitDelimiter string
	WrodsConfidence    int64
	MaxSampleCnt       int
	//MaxTopKeyCnt       int
	MaxStatKeyPartten  int
	GuseKeySampleEntry *GuseKeySample
	StatKeyPartten     map[string]*KeyPartten
	regRule            []*RegItem
	SepcialRule        []*RegItem
	DelimiterAuto      []string
	StatsTotal         int64
	AdjustFactor       int64
	EnableMoreData     bool
	redisCon           *db.RedisContext
}

type RegItem struct {
	Mode string
	Reg  *regexp.Regexp
}

//func NewGuessKeyMode(samples, confidence, adjfactor int64, sample, maxtop int, delimiter string) *GuessKeyMode {
func NewGuessKeyMode(samples, confidence, adjfactor int64, sample int, delimiter string) *GuessKeyMode {
	return &GuessKeyMode{
		MaxGusessCnt:    samples,
		SplitDelimiter:  delimiter,
		WrodsConfidence: confidence,
		MaxSampleCnt:    sample,
		//MaxTopKeyCnt:    maxtop,
		StatsTotal:   0,
		AdjustFactor: adjfactor,
		GuseKeySampleEntry: &GuseKeySample{
			KeySamples:       map[string]*KeyStatInfo{},
			TmpWordsSample:   map[string]int64{},
			KnowedSmartWrods: map[string]int64{},
		},
		StatKeyPartten: map[string]*KeyPartten{},
		regRule: []*RegItem{&RegItem{Mode: "([0~9]+)", Reg: regexp.MustCompile(`^[[:digit:]]+$`)},
			&RegItem{Mode: "([A~Za~z]+)", Reg: regexp.MustCompile(`^[[:alpha:]]+$`)},
			&RegItem{Mode: "([A~Za~z0~9]+)", Reg: regexp.MustCompile(`[[:alnum:]]+`)},
			&RegItem{Mode: "([.*]+)", Reg: regexp.MustCompile(`.+`)}},
		SepcialRule: []*RegItem{
			&RegItem{Mode: "([YYYYMMDD])", Reg: regexp.MustCompile(`^[D|d]*20[0-9]{2}(0[1-9]|1[0-2])([0-2][0-9]|3[0-1])$`)},
			&RegItem{Mode: "([YYYYMM])", Reg: regexp.MustCompile(`^20[0-9]{2}(0[0-9]|1[0-2])$`)}}, //---here has Miscarriage of Justice
		//&RegItem{Mode: "([dateLong])", Reg: regexp.MustCompile(`^15[0-9]{8,10}$`)}},
		DelimiterAuto: []string{"|", "_", ":", "#", "."},
	}
}

type GuseKeySample struct {
	KeySamples       map[string]*KeyStatInfo
	KnowedSmartWrods map[string]int64
	TmpWordsSample   map[string]int64
	GuseCnt          int64
}

type KeyStatInfo struct {
	*comm.FileContent
	Cnt int64
}

type KeyPartten struct {
	KeyType        string   `json:"key_type"`
	KeySamples     []string `json:"samples"`
	KeyModes       string   `json:"partten"`
	keyStatsMode   string   `json:"statsMode"`
	KeyTotalCnt    int64    `json:"count"`
	KeySampleRaito float32  `json:"sample_raito"`
	MaxFields      int64    `json:"max_fileds"`
	MaxValSize     int64    `json:"max_valsize"`
	//MaxKeyMeta []*KeyMeta `json:"max_key_meta"`
	AvgFileds  int64 `json:"avg_fileds"`
	AvgValSize int64 `json:"avg_valsize"`
	KeyExpired int64 `json:"keys_expired"`
	KeyNoTtl   int64 `json:"keys_nottl"`
}

type KeyMeta struct {
	Key     string `json:"key"`
	Fields  int64  `json:"fileds"`
	ValSzie int64  `json:"valsize"`
}

func (this *GuessKeyMode) Run(ctx context.Context, keyCh chan comm.FileContent) {
	for {
		select {
		case k, ok := <-keyCh:
			if !ok {
				this.CloseUpChildPartten()
				return
			}
			log.Debugf("caculate string partten : %s", k.Key)
			this.StatsTotal++
			if ok := this.WordsLearning(k); ok {
				this.GuessKeyPartten(false)
				//bx, _ := json.Marshal(this.GuseKeySampleEntry)
				//log.Debugf("I'am Done :%d || GuesKeyInfo :%s ", this.StatsTotal, bx)
			}
			//if (this.StatsTotal % 10000) == 0 {
			//	this.AdjustFactor++
			//}
			if (this.StatsTotal % int64(this.MaxStatKeyPartten)) == 0 {
				if len(this.StatKeyPartten) > this.MaxStatKeyPartten {
					var deled int64
					this.CloseUpChildPartten()
					for _, skp := range this.StatKeyPartten {
						if skp.KeyTotalCnt < 2 {
							deled++
							delete(this.StatKeyPartten, skp.KeyModes)
						}
					}
					if deled > 0 {
						log.Infof("deleted small than 2 partten %d::%d!", len(this.StatKeyPartten), deled)
					}
				}
				if len(this.StatKeyPartten) > this.MaxStatKeyPartten*2 {
					var removed int
					keyModes := []KeyPartten{}
					for _, kpp := range this.StatKeyPartten {
						keyModes = append(keyModes, *kpp)
					}
					sort.Sort(SortableKeyMode(keyModes)) //decr
					for i := len(keyModes) - 1; len(this.StatKeyPartten) > this.MaxStatKeyPartten; i-- {
						removed++
						log.Debugf("delete key partten %d: %#v", removed, keyModes[i])
						//delete(this.StatKeyPartten, keyModes[i].KeyModes)
						delete(this.StatKeyPartten, keyModes[i].keyStatsMode)
					}
					log.Warnf("do memory Controll ; deleted [%3d] small raito partten", removed)
				}
			}
			//case <-ctx.Done():
			//	//return
			//	goto TAG_END_LOOP
		}
	}
}

func (this *GuessKeyMode) SetInstanceInfo(addr, paswd string) {
	if addr == "" {
		this.EnableMoreData = false
		return
	}
	redisConn := db.NewRedis(addr, paswd)
	_, err := redisConn.Ttl("conn_test_vito_key")
	if err != nil {
		log.Warnf("Conn redis %s:%s failed : %v", addr, paswd, err)
		this.EnableMoreData = false
		return
	}
	this.redisCon = redisConn
}
func (this *GuessKeyMode) SetMaxKeyParttened(m int) {
	if m > 0 {
		this.MaxStatKeyPartten = m
	} else {
		this.MaxStatKeyPartten = 2000
	}
}
func (this *GuessKeyMode) SetKnownSmartsWords(w string) {
	err := json.Unmarshal([]byte(w), &this.GuseKeySampleEntry.KnowedSmartWrods)
	if err != nil {
		log.Warnf("load known smart words failed %s : %v", w, err)
	}

}
func (this *GuessKeyMode) SetStatFromFile(s bool) {
	this.EnableMoreData = false
	this.StatFromFile = s
}
func (this *GuessKeyMode) GuessDelimiter(k string) string {
	var max, firstIdx int
	var maxd string
	firstIdx = 999999
	for _, d := range this.DelimiterAuto {
		ws := strings.Split(k, d)
		t_fristIdx := len(ws[0])
		//log.Debugf("for k : %s ; delimiter : %s ; args : splen :curmax:%d %d,fidx: %d", k, d, max, len(ws), t_fristIdx)
		if len(ws) >= max && t_fristIdx < firstIdx {
			max = len(ws)
			maxd = d
			firstIdx = t_fristIdx
		}
	}
	log.Debugf("for k %s , delimiter is : [%s]", k, maxd)
	return maxd

}

func (this *GuessKeyMode) AutoDelimiter(k string) ([]string, string) {
	if this.SplitDelimiter == "auto" {
		this.TrueSplitDelimiter = this.GuessDelimiter(k)
	} else {
		this.TrueSplitDelimiter = this.SplitDelimiter
	}
	words := strings.Split(k, this.TrueSplitDelimiter)
	return words, this.TrueSplitDelimiter
}

func (this *GuessKeyMode) WordsLearning(s comm.FileContent) bool {
	words, _ := this.AutoDelimiter(s.Key)
	if this.GuseKeySampleEntry.GuseCnt < this.MaxGusessCnt {
		this.GuseKeySampleEntry.GuseCnt++
		for _, w := range words {
			for _, reg := range this.SepcialRule {
				if reg.Reg.MatchString(w) {
					w = reg.Mode
					break
				}
			}
			if _, ok := this.GuseKeySampleEntry.KnowedSmartWrods[w]; !ok {
				if _, ok := this.GuseKeySampleEntry.TmpWordsSample[w]; !ok {
					this.GuseKeySampleEntry.TmpWordsSample[w] = 1
				} else {
					this.GuseKeySampleEntry.TmpWordsSample[w]++
				}
			}
		}
		this.GuseKeySampleEntry.KeySamples[s.Key] = &KeyStatInfo{
			FileContent: &s,
			Cnt:         0,
		}
		//b, _ := json.Marshal(this.GuseKeySampleEntry.TmpWordsSample)
		//log.Infof("after key : %s , words : %s", s, b)
		return false
	}

	//recaculate words konwing
	log.Debugf("delete sample_words factor small than %d :::", this.AdjustFactor)
	for w, c := range this.GuseKeySampleEntry.TmpWordsSample {
		if c < this.AdjustFactor {
			delete(this.GuseKeySampleEntry.TmpWordsSample, w)
		}
	}
	log.Debugf("sample_words after adjust : %#v", this.GuseKeySampleEntry.TmpWordsSample)
	//b, _ := json.Marshal(this.GuseKeySampleEntry.KeySamples)
	log.Debugf("todo key sample is : %+v", this.GuseKeySampleEntry.KeySamples)
	return true
}

//called by locked_func
func (this *GuessKeyMode) GetStringModle(w string) (string, bool) {
	//NOTE(xiepaup) for learning history , in history ; think as good
	if _, ok := this.GuseKeySampleEntry.KnowedSmartWrods[w]; ok {
		return w, true
	}

	if _, ok := this.GuseKeySampleEntry.TmpWordsSample[w]; !ok {
		for _, reg := range this.SepcialRule {
			if reg.Reg.MatchString(w) {
				//return reg.Mode, true
				return reg.Mode, false
				//return reg.Mode, true //caculate spec to known words ???
			}
		}
		for _, reg := range this.regRule {
			if reg.Reg.MatchString(w) {
				return reg.Mode, false
			}
		}
		if w != "" {
			w = "([.*]+)"
		}
	} else {
		if this.GuseKeySampleEntry.TmpWordsSample[w] > this.WrodsConfidence {
			return w, true
		}
	}
	return w, false
}

func (this *GuessKeyMode) GuessKeyPartten(finall bool) {
	log.Debugf("begin do guess key partten round ...")
	for _, k := range this.GuseKeySampleEntry.KeySamples {
		tmp_partten, chgd := this.GetSingleKeyPartten(k.Key, finall)
		if !chgd {
			continue
		}
		stp := fmt.Sprintf("%s%s", k.KType, tmp_partten)
		delete(this.GuseKeySampleEntry.KeySamples, k.Key)
		if _, ok := this.StatKeyPartten[stp]; !ok {
			kpinfo := &KeyPartten{
				KeySamples:   []string{},
				KeyModes:     tmp_partten,
				keyStatsMode: stp,
				KeyType:      k.KType,
			}
			this.StatKeyPartten[stp] = kpinfo
		}
		if this.MaxSampleCnt > len(this.StatKeyPartten[stp].KeySamples) {
			this.StatKeyPartten[stp].KeySamples = append(this.StatKeyPartten[stp].KeySamples, k.Key)
		}
		if k.WhenExp == 0 {
			this.StatKeyPartten[stp].KeyNoTtl++
		} else if k.WhenExp <= CUR_AFTER_10MINS_TIME {
			this.StatKeyPartten[stp].KeyExpired++
		}
		this.StatKeyPartten[stp].AvgFileds += k.ValFileds
		this.StatKeyPartten[stp].AvgValSize += k.ValSize
		if k.ValSize > this.StatKeyPartten[stp].MaxValSize {
			this.StatKeyPartten[stp].MaxValSize = k.ValSize
		}

		if k.ValFileds > this.StatKeyPartten[stp].MaxFields {
			this.StatKeyPartten[stp].MaxFields = k.ValFileds
		}
		this.StatKeyPartten[stp].KeyTotalCnt++

	}
	this.GuseKeySampleEntry.GuseCnt = int64(len(this.GuseKeySampleEntry.KeySamples))
}

func (this *GuessKeyMode) GetSingleKeyPartten(k string, finall bool) (string, bool) {
	var (
		//repushwords bool
		tmp_partten string
		transCount  int
	)
	words, _ := this.AutoDelimiter(k)
	wordsHalfLen := len(words) / 2
	for _, w := range words {
		ttp, inSpec := this.GetStringModle(w)
		log.Debugf("partten for string %s <=> %s :[%d]: %v", w, ttp, this.WrodsConfidence, inSpec)
		if inSpec {
			transCount++
		}
		tmp_partten = fmt.Sprintf("%s%s%s", tmp_partten, this.TrueSplitDelimiter, ttp)
	}
	//for 6578cce3e9cad04xc7b011sddb1ee7e6f0c these keys , cloud no transfer!!! any more
	if len(words) == 1 || this.GuseKeySampleEntry.KeySamples[k].Cnt >= 10 {
		tmp_partten = tmp_partten[len(this.TrueSplitDelimiter):]
		log.Debugf("caculated key : %s :: partten :%s ||=!([.*]+),caculated : %d", k, tmp_partten, this.GuseKeySampleEntry.KeySamples[k].Cnt)
		//return fmt.Sprintf("%s%s", this.TrueSplitDelimiter, "([.*]+)"), true
		return tmp_partten, true
	}
	log.Debugf("caculated key : %s :: partten : %s", k, tmp_partten)
	//idata|([0~9]+)|([0~9]+)|([YYYYMMDD])|([0~9]+)
	if (transCount < wordsHalfLen) && !finall {
		log.Debugf("caculated key : %s :: partten %s , [%d] no greater than half , push back", k, tmp_partten, transCount)
		//here should do recaculate smart words !
		for _, w := range words {
			if _, ok := this.GuseKeySampleEntry.TmpWordsSample[w]; !ok {
				this.GuseKeySampleEntry.TmpWordsSample[w] = 1
			} else {
				this.GuseKeySampleEntry.TmpWordsSample[w]++
			}
		}
		this.GuseKeySampleEntry.KeySamples[k].Cnt += 1
		return tmp_partten, false
	} else {
		if finall && transCount == 0 {
			tmp_partten = "[.*]+"
		} else {
			tmp_partten = tmp_partten[len(this.TrueSplitDelimiter):]
		}
	}
	//finall partten for key : 93505:status:47193839482911049 : :93403:rankforbid:[0-9a-z]
	log.Debugf("finall caculated key : %s :: partten %s", k, tmp_partten)
	return tmp_partten, true
}

func (this *GuessKeyMode) GetSampledKeys() map[string]*KeyPartten {
	b, _ := json.Marshal(this.GuseKeySampleEntry)
	log.Debugf("do recaculate smart words : %s", b)
	for w, c := range this.GuseKeySampleEntry.TmpWordsSample {
		//if c.Cnt < this.AdjustFactor && c.Used <= 0 {
		if c < this.AdjustFactor {
			delete(this.GuseKeySampleEntry.TmpWordsSample, w)
		}
	}
	bb, _ := json.Marshal(this.GuseKeySampleEntry.TmpWordsSample)
	log.Debugf("finall smart words : %s", bb)
	this.GuessKeyPartten(true)
	//for kp, _ := range this.StatKeyPartten {
	//	if this.StatKeyPartten[kp].KeyTotalCnt < 2 {
	//		for _, k := range this.StatKeyPartten[kp].KeySamples {
	//			this.GuseKeySampleEntry.KeySamples[k] = 0
	//		}
	//		delete(this.StatKeyPartten, kp)
	//	}
	//}
	return this.StatKeyPartten
}

func (this *GuessKeyMode) GetSmartWords() map[string]int64 {
	words := map[string]int64{}
	for k, c := range this.GuseKeySampleEntry.TmpWordsSample {
		if _, ok := words[k]; !ok {
			//words[k] = &SmartWords{Cnt: c.Cnt, Used: c.Used}
			words[k] = 1
		} else {
			words[k] += c
		}
	}
	return words
}

//NOTE(xiepaup) only show top key parttens ...
func (this *GuessKeyMode) GetTopNKeys(top int) []KeyPartten {
	var total int64
	keyModes := []KeyPartten{}
	topkeyModes := []KeyPartten{}
	this.CloseUpChildPartten()
	for _, kpp := range this.StatKeyPartten {
		keyModes = append(keyModes, *kpp)
		total += kpp.KeyTotalCnt
	}
	sort.Sort(SortableKeyMode(keyModes))
	for i := 0; i < top && i < len(keyModes); i++ {
		keyModes[i].KeySampleRaito = float32(keyModes[i].KeyTotalCnt) / float32(total) * 100
		keyModes[i].AvgFileds = int64(float64(keyModes[i].AvgFileds) / float64(keyModes[i].KeyTotalCnt))
		keyModes[i].AvgValSize = int64(float64(keyModes[i].AvgValSize) / float64(keyModes[i].KeyTotalCnt))
		topkeyModes = append(topkeyModes, keyModes[i])
	}
	return topkeyModes
}

func (this *GuessKeyMode) PolymerizationSampledKeys(sks ...map[string]*KeyPartten) {
	for _, sks_one := range sks {
		for f, kpp := range sks_one {
			if _, ok := this.StatKeyPartten[f]; !ok {
				this.StatKeyPartten[f] = kpp
				continue
			}
			if kpp.MaxFields > this.StatKeyPartten[f].MaxFields {
				this.StatKeyPartten[f].MaxFields = kpp.MaxFields
			}
			if kpp.MaxValSize > this.StatKeyPartten[f].MaxValSize {
				this.StatKeyPartten[f].MaxValSize = kpp.MaxValSize
			}
			this.StatKeyPartten[f].KeyTotalCnt += kpp.KeyTotalCnt
			this.StatKeyPartten[f].AvgFileds += kpp.AvgFileds
			this.StatKeyPartten[f].AvgValSize += kpp.AvgValSize
			this.StatKeyPartten[f].KeyExpired += kpp.KeyExpired
			this.StatKeyPartten[f].KeyNoTtl += kpp.KeyNoTtl
			this.StatKeyPartten[f].AvgValSize += kpp.AvgValSize
			this.StatKeyPartten[f].AvgValSize += kpp.AvgValSize
			for _, s := range kpp.KeySamples {
				if len(this.StatKeyPartten[f].KeySamples) >= this.MaxSampleCnt {
					break
				}
				this.StatKeyPartten[f].KeySamples = append(this.StatKeyPartten[f].KeySamples, s)
			}
		}
	}
}

func (this *GuessKeyMode) PolymerizationSmartWords(smarts ...map[string]int64) map[string]int64 {
	//words := map[string]int64{}

	smarts = append(smarts, this.GuseKeySampleEntry.TmpWordsSample)
	for _, smart := range smarts {
		for k, c := range smart {
			if _, ok := this.GuseKeySampleEntry.KnowedSmartWrods[k]; !ok {
				this.GuseKeySampleEntry.KnowedSmartWrods[k] = c
			} else {
				this.GuseKeySampleEntry.KnowedSmartWrods[k] += c
			}
		}
	}
	return this.GuseKeySampleEntry.KnowedSmartWrods
}

type SortableKeyMode []KeyPartten

func (this SortableKeyMode) Len() int           { return len(this) }
func (this SortableKeyMode) Swap(i, j int)      { this[i], this[j] = this[j], this[i] }
func (this SortableKeyMode) Less(i, j int) bool { return this[i].KeyTotalCnt > this[j].KeyTotalCnt }
