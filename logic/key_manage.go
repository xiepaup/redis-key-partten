package logic

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"regexp"
	"strings"
	//"sync"
)

type GuessKeyMode struct {
	MaxGusessCnt       int64
	SplitDelimiter     string
	TrueSplitDelimiter string
	WrodsConfidence    int64
	MaxSampleList      int
	GuseKeySampleEntry *GuseKeySample
	StatKeyPartten     map[string]*KeyPartten
	regRule            []*RegItem
	SepcialRule        []*RegItem
	DelimiterAuto      []string
	StatsTotal         int64
	AdjustFactor       int64
}

type RegItem struct {
	Mode string
	Reg  *regexp.Regexp
}

func NewGuessKeyMode(samples, confidence int64, sample int, delimiter string) *GuessKeyMode {
	return &GuessKeyMode{
		MaxGusessCnt:    samples,
		SplitDelimiter:  delimiter,
		WrodsConfidence: confidence,
		MaxSampleList:   sample,
		StatsTotal:      0,
		AdjustFactor:    4,
		GuseKeySampleEntry: &GuseKeySample{
			KeySamples:       map[string]struct{}{},
			TmpWordsSample:   map[string]int64{},
			KnowedSmartWrods: map[string]struct{}{},
		},
		StatKeyPartten: map[string]*KeyPartten{},
		regRule: []*RegItem{&RegItem{Mode: "[0-9]+", Reg: regexp.MustCompile(`^[[:digit:]]+$`)},
			&RegItem{Mode: "[A-Za-z]+", Reg: regexp.MustCompile(`^[[:alpha:]]+$`)},
			&RegItem{Mode: "[0-9A-Za-z]+", Reg: regexp.MustCompile(`[[:alnum:]]+`)},
			&RegItem{Mode: "[0-9A-Za-z_]+", Reg: regexp.MustCompile(`[[:word:]]+`)}},
		SepcialRule: []*RegItem{&RegItem{Mode: "[YYYMMDD]", Reg: regexp.MustCompile(`^20[0-9]{2}[0|1][0-9][0-3][0-9]$`)},
			&RegItem{Mode: "[YYYMM]", Reg: regexp.MustCompile(`^20[0-9]{2}[0|1][0-9]$`)},
			&RegItem{Mode: "[date_long]", Reg: regexp.MustCompile(`^15[0-9]{8,10}$`)}},
		DelimiterAuto: []string{"|", "_", ":", "#"},
	}
}

type GuseKeySample struct {
	KeySamples       map[string]struct{}
	KnowedSmartWrods map[string]struct{}
	TmpWordsSample   map[string]int64
	GuseCnt          int64
}

type KeyPartten struct {
	KeySamples      []string `json:"samples"`
	KeyModes        string   `json:"partten"`
	KeyTotalCnt     int64    `json:"count"`
	keyTotalFileds  int64    `json:"fields_total"`
	keyTotalValSize int64    `json:"valsize_total"`
}

func (this *GuessKeyMode) Run(ctx context.Context, keyCh chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case k := <-keyCh:
			this.StatsTotal++
			if ok := this.WordsLearning(k); ok {
				this.GuessKeyPartten(false)
			}
			if (this.StatsTotal % 500000) == 0 {
				this.AdjustFactor++
			}
		}
	}
}
func (this *GuessKeyMode) GuessDelimiter(k string) string {
	var max, firstIdx int
	var maxd string
	firstIdx = 999999
	for _, d := range this.DelimiterAuto {
		ws := strings.Split(k, d)
    //NOTE(xiepaup) here for have equal delimiter ; add privilege for delimiter
		t_fristIdx := len(ws[0])
		log.Debugf("for k : %s ; delimiter : %s ; args : splen :curmax:%d %d,fidx: %d", k, d, max, len(ws), t_fristIdx)
		if len(ws) >= max && t_fristIdx < firstIdx {
			max = len(ws)
			maxd = d
			firstIdx = t_fristIdx
		}
	}
	log.Debugf("for k %s , delimiter is : %s", k, maxd)
	return maxd

}

func (this *GuessKeyMode) AutoDelimiter(k string) ([]string, string) {
	if this.SplitDelimiter == "auto" {
		this.TrueSplitDelimiter = this.GuessDelimiter(k)
	} else {
		this.TrueSplitDelimiter = this.SplitDelimiter
	}
	words := strings.Split(k, this.TrueSplitDelimiter)
	return words, fmt.Sprintf("%d%s%d", len(words), this.TrueSplitDelimiter, len(words))
}

func (this *GuessKeyMode) WordsLearning(s string) bool {
	log.Debugf("Distinguish string partten : %s", s)
	words, _ := this.AutoDelimiter(s)
	if this.GuseKeySampleEntry.GuseCnt < this.MaxGusessCnt {
		this.GuseKeySampleEntry.GuseCnt++
		for _, w := range words {
			//use less memory
			for _, reg := range this.SepcialRule {
				if reg.Reg.MatchString(w) {
					//ts = strings.Replace(s, w, reg.Mode, 1)
					w = reg.Mode
					break
				}
			}
			this.GuseKeySampleEntry.TmpWordsSample[w]++
		}
		this.GuseKeySampleEntry.KeySamples[s] = struct{}{}
		return false
	}

	//recaculate words konwing
	log.Debugf("words konwing : %#v", this.GuseKeySampleEntry.TmpWordsSample)
	for w, c := range this.GuseKeySampleEntry.TmpWordsSample {
		if c < this.AdjustFactor {
			delete(this.GuseKeySampleEntry.TmpWordsSample, w)
		}
	}
	log.Debugf("words konwing after adjust : %#v", this.GuseKeySampleEntry.TmpWordsSample)
	b, _ := json.Marshal(this.GuseKeySampleEntry.KeySamples)
	log.Debugf("todo key sample is : %s", b)
	return true
}

//called by locked_func
func (this *GuessKeyMode) GetStringModle(w string) (tmp_partten string, transfered bool) {
	//b, _ := json.Marshal(this.GuseKeySampleEntry[gks].TmpWordsSample)
	//log.Debugf("<<<<<<<todo key sample is : %s", b)

	//NOTE(xiepaup) for learning history , in history ; think as good
	if _, ok := this.GuseKeySampleEntry.KnowedSmartWrods[w]; ok {
		return w, true
	}

	if _, ok := this.GuseKeySampleEntry.TmpWordsSample[w]; !ok {
		var matched bool
		for _, reg := range this.regRule {
			if reg.Reg.MatchString(w) {
				tmp_partten = reg.Mode
				matched = true
				break
			}
		}
		if !matched {
			if w == "" {
				tmp_partten = ""
			} else {
				tmp_partten = "UNKOWN_PARTTEN"
			}
		}
	} else {
		if this.GuseKeySampleEntry.TmpWordsSample[w] > this.WrodsConfidence {
			tmp_partten = w
			transfered = true
		} else {
			tmp_partten = w
		}
	}
	return
}

func (this *GuessKeyMode) GuessKeyPartten(finall bool) {
	var repushwords bool
	//mutex.Lock()
	for k, _ := range this.GuseKeySampleEntry.KeySamples {
		//words := strings.Split(k, this.SplitDelimiter)
		words, _ := this.AutoDelimiter(k)
		//gks := fmt.Sprintf("%d%s%d", len(words), this.SplitDelimiter, len(words))
		var tmp_partten string
		var transfered bool
		for _, w := range words {
			for _, reg := range this.SepcialRule {
				if reg.Reg.MatchString(w) {
					w = reg.Mode
					break
				}
			}
			ttp, t := this.GetStringModle(w)
			log.Debugf("partten for string %s <=> %s :[%d]: %v", w, ttp, this.WrodsConfidence, t)
			if t {
				transfered = true
			}
			tmp_partten = fmt.Sprintf("%s%s%s", tmp_partten, this.TrueSplitDelimiter, ttp)
		}
		log.Debugf("key : %s :: partten : %s", k, tmp_partten)
		if !transfered && !finall {
			repushwords = true
			//here should do recaculate smart words !
			for _, w := range words {
				this.GuseKeySampleEntry.TmpWordsSample[w]++
			}
			continue
		} else {
			if finall && !transfered {
				tmp_partten = k
			} else {
				tmp_partten = tmp_partten[len(this.TrueSplitDelimiter):]
			}
		}
		//finall partten for key : 9305:status:471721393834893049 : :9403:rankforbid:[0-9a-z]
		log.Debugf("====>finall partten for key : %s : %s", k, tmp_partten)

		delete(this.GuseKeySampleEntry.KeySamples, k)
		if _, ok := this.StatKeyPartten[tmp_partten]; !ok {
			this.StatKeyPartten[tmp_partten] = &KeyPartten{KeySamples: []string{}}
		}
		if this.MaxSampleList > len(this.StatKeyPartten[tmp_partten].KeySamples) {
			this.StatKeyPartten[tmp_partten].KeySamples = append(this.StatKeyPartten[tmp_partten].KeySamples, k)
		}
		this.StatKeyPartten[tmp_partten].KeyTotalCnt++
		this.StatKeyPartten[tmp_partten].KeyModes = tmp_partten
	}
	this.GuseKeySampleEntry.GuseCnt = int64(len(this.GuseKeySampleEntry.KeySamples))
	bb, _ := json.Marshal(this.StatKeyPartten)
	if repushwords {
		for w, c := range this.GuseKeySampleEntry.TmpWordsSample {
			if c < 2 {
				delete(this.GuseKeySampleEntry.TmpWordsSample, w)
			}
		}
	}
	//mutex.Unlock()
	log.Debugf("Neweast key sample is : %s", bb)
	bb, _ = json.Marshal(this.GuseKeySampleEntry.TmpWordsSample)
	log.Debugf("===>>smart words : %s", bb)
}

func (this *GuessKeyMode) GetSampledKeys() map[string]*KeyPartten {
	b, _ := json.Marshal(this.GuseKeySampleEntry)
	log.Debugf("do recaculate smart words : %s", b)
	for w, c := range this.GuseKeySampleEntry.TmpWordsSample {
		if c < this.AdjustFactor {
			delete(this.GuseKeySampleEntry.TmpWordsSample, w)
		}
	}
	bb, _ := json.Marshal(this.GuseKeySampleEntry.TmpWordsSample)
	log.Debugf("finall smart words : %s", bb)
	this.GuessKeyPartten(true)
	for kp, _ := range this.StatKeyPartten {
		if this.StatKeyPartten[kp].KeyTotalCnt < 2 {
			for _, k := range this.StatKeyPartten[kp].KeySamples {
				this.GuseKeySampleEntry.KeySamples[k] = struct{}{}
			}
			delete(this.StatKeyPartten, kp)
		}
	}
	return this.StatKeyPartten
}

func (this *GuessKeyMode) GetSmartWords() map[string]int64 {
	words := map[string]int64{}
	for k, c := range this.GuseKeySampleEntry.TmpWordsSample {
		if _, ok := words[k]; !ok {
			words[k] = c
		} else {
			words[k] += c
		}
	}
	return words
}

func (this *GuessKeyMode) ShowSampledKeys() {
	var i int64
	for f, kpp := range this.StatKeyPartten {
		i++
		b, _ := json.Marshal(kpp)
		log.Infof("%5d %s Parttend : %s", i, f, b)
	}
}

func (this *GuessKeyMode) PolymerizationSampledKeys(sks ...map[string]*KeyPartten) {
	for _, sks_one := range sks {
		for f, kpp := range sks_one {
			if _, ok := this.StatKeyPartten[f]; !ok {
				this.StatKeyPartten[f] = kpp
				continue
			}
			this.StatKeyPartten[f].KeyTotalCnt += kpp.KeyTotalCnt
			this.StatKeyPartten[f].keyTotalFileds += kpp.keyTotalFileds
			this.StatKeyPartten[f].keyTotalValSize += kpp.keyTotalValSize
			for _, s := range kpp.KeySamples {
				if len(this.StatKeyPartten[f].KeySamples) > this.MaxSampleList {
					break
				}
				this.StatKeyPartten[f].KeySamples = append(this.StatKeyPartten[f].KeySamples, s)
			}
		}
	}
}

func (this *GuessKeyMode) PolymerizationSmartWords(smarts ...map[string]int64) map[string]int64 {
	words := map[string]int64{}
	for _, smart := range smarts {
		for k, c := range smart {
			if _, ok := words[k]; !ok {
				words[k] = c
			} else {
				words[k] += c
			}
		}
	}
	return words
}
