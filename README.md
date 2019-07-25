
# Redis key生命周期管理-key的聚合分析

> Redis中的数据以k-v的方式组织；为了方便管理key一般具有某些特定的模式；有些key直接是由mysql中的表行数据转化而来；如果要对Redis的key进行生命周期管理，由于Redis的key量一般非常大（通常上亿），那么需要把这些key还原成它原来的模式，进而对key的什么周期管理演变成对特定模型的key什么周期管理。

-------------------


## 想要解决的问题

- 想要知道某些key增长比例
- 想要计算某些key的存储占用的成本


通过这个工具我们可以了解一个实例中哪种key占用的比例是怎样的，但是我还想知道更多东西，比如这种类型的key 的samples 方便我们直观的知道这些可以都张什么样子，而有些key通常会由些业务id，等我不希望被聚合掉，而是希望被保留下来，于是就开始着手进行工具优化工作；

先来看一些key 的栗子：
> 93031:friendpresent:1017375929259
93033:friendpresent:10183933368631
93051:friendpresent:102929323828455
global:userrecentrole:odfjsdjLtqV0zu6vIidjdslGhvLGvlrmNhoS8M
global:userrecentrole:oLdfslsLSKJD0zuLdfjsslsLSKJD0zudj02SALlrSW7YZK2FereYw
global:userrecentrole:dfjsdjoLtqV0zuzIVadwSDFJslsLSKJD0zuD02oSohVsCCTs1S8
area:userarea:0C36D3EA39BBD4D2F553SIDFJKslsLSKJD0zu9LDF21001DE008A1B734

我的目标是想要得出这样子的结果：

> 需要把 
`"area:usercredit:0C36DD4D2F553SDFDLFDOFDEISLIDFJK9LDF21001DE008A1B734"`
`"area:usercredit:00C3EA39BBC4C6AD3S2F553SDFDLFDOFDEIDFJK9LDF21E0832"`
`"area:usercredit:013113SIDFJK9LD2F553SDFDLFDOFDEF21418ADDA428F2F8"` 
这些key归为一类「area:usercredit:[0-9a-z] 」；
而把
`"93033:rolesignature:1000129297242"`
`"93033:rolesignature:1072583033970"`
`"93033:rolesignature:10852038320538"`
这些key归为一类「93033:rolesignature:[0-9] 」；

## 求字符串相似度

> 首先想到的就是计算这些key的相似度， 把相似度接近的归为同一类key

### Levenshtein Distance 算法
> 是指两个字符串之间，由一个转成另一个所需的最少编辑操作次数。许可的编辑操作包括将一个字符替换成另一个字符，插入一个字符，删除一个字符。一般来说，编辑距离越小，两个串的相似度越大。

根据这个算法的定义， 用golang版本实现之后，拿了两个字符串进行计算，但是结果不太满意：
>   a := "tdw:112729:20181219:djfsd982lsd289jdksfj0flksadfjsdf2lkfsadh9fasddfyf"
    b := "tdw:112729:20190323:9dkfk892o3kd9sdfa9dfakdfj92fklsdahf"
 - 通过直接计算两个字符串的相似度为： `40` 低
 -  通过加权后优化的算法，再次计算相似度为： `74` 比较低
 -  调整权重，越靠前的filed权重提高，再次计算相似度为： `94` 比较理想
 
> 通过结果发现，这两个字符串在我们认为可以归位同一类，但是得到的结果 为 `40` ，表明相似度较低，不能归为同一类，猜想可能是由于 后边一堆随机的字符串影响了计算结果。

> 于是做算法调优，根据分隔符划分key，整个key的相似度 = 各field的相似度之和；并对各field做加权计算，越靠key前边的字符串相似度计算出的结果权重越大，越靠后字符串相似度计算结果权重越小，并通过多次调整权重，最终的到了一个比较满意的结果`94`。


相似度尽管已经达到了期望值，但是毕竟是针对这种类型的key进行优化调整的，比较难有普适性，并且这种算法有一个规则：`越靠前的filed权重越大`。如果把这种随机字符串提前，比如:
>   a := "tdw:djfsd982lsd289jdksfj0flksadfjsdf2lkfsadh9fasddfyf:112729:20181219"
    b := "tdw:9dkfk892o3kd9sdfa9dfakdfj92fklsdahf:112729:20190323"
通过这个算法计算出来的相似度又会很低，从而认为它们不属于一类； 但是如果人来看的话， 这就是同一类key。


### 辨别随机字符串

> 编辑距离算法结果会被 随机字符串干扰，那么有没有办法识别出这种随机字符串呢？这种字符串都是些什么东西？

#### base64编解码
通过了解这种字符串一般是一些openid，如果能有办法知道它确实是一个openid，那么我就可以认为它就是随机字符串；这种字符串通常是有固定的长度，而有一个种办法判断是否为base64 数据，判断方法是:`base64只包含特定字符;解码再转码，查验是否相等(不能判断一定是，可以判断一定不是)`; 似乎是解决了一个问题了，但是所有的这种都是openid，或者都是符合base64编码的么？如果不能确定都是， 那么这个算发也不具有普适性。

>  func JudgeBase64(str string) bool {
    pattern := "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
    matched, err := regexp.MatchString(pattern, str)
    if err != nil {
        return false
    }
    if !(len(str)%4 == 0 && matched) {
        return false
    }
    unCodeStr, err := base64.StdEncoding.DecodeString(str)
    if err != nil {
        return false
    }
    tranStr := base64.StdEncoding.EncodeToString(unCodeStr)
    if str == tranStr {
        return true
    }
    return false
}


#### 频数测试
通过`测试二进制序列中，“0”和“1” 数目是否近似相等。如果是，则序列是随机的`
先把字符串转换成二进制序列，然后在计算二进制序列中的0，1 比例:


字符串：dfjsdjoLtqV0zuzIVadwSDFJD02oSohVsCCTs1S8 
二进制：01100100011001100110101001110011011001000110101001101111010011000111010001110001010101100011000001111010011101010111101001001001010101100110000101100100011101110101001101000100010001100100101001000100001100000011001001101111010100110110111101101000010101100111001101000011010000110101010001110011001100010101001100111000 
0/1比例： 1.1192054 `「0数量：169  ； 1数量：151」`
字符串：8093fb4907c041b6863b28ec5d9275b0 
二进制：0011100000110000001110010011001101100110011000100011010000111001001100000011011101100011001100000011010000110001011000100011011000111000001101100011001101100010001100100011100001100101011000110011010101100100001110010011001000110111001101010110001000110000
0/1比例：  1.3486239  `「0数量：147 ；1数量：109」`

显然这样也不太靠谱，两个字符串测试结果误差范围太大。
通过进一步的查阅资料，发现涉及到的东西越越有意思，但是真需要这么麻烦么？ 是否想多了？

> 如何确定一列数是否随机？
标签： 统计学 数学 算法 密码

### 程序的自我学习
既然没有一个简单的办法知道一个字符串是否是随机的字符串，那么需要再结合业务场景进行进一步的思考：`我们是要在一个实例中找出具有相似的key，聚合出同模式的key的集合` ，那么我们可以把辨别是否随机字符这个事情放到实例的唯独来辨别，而不是放到具体key来辨别。
具体思想是：首先读入一堆keys，然后根据规则把这些key进行拆分，并进行频数统计，定期清理掉频数低的filed(减少内存占用)，当样本达到一定数量时，再进行key归类，归类时，给每个filed设置一定的置信度，到达置信度的filed进行原样保留，否则进行保留符替换。

效果：
>  19306:leagueinstancedata:[0-9]:1135001 : {"KeySamples":["19306:leagueinstancedata:127229214682:1135001","19306:leagueinstancedata:25230921466522:1135001","19306:leagueinstancedata:29214684386394:1135001"],"KeyTotalCnt":199}
92404:timestamp:[0-9] : {"KeySamples":["92404:timestamp:103720124","92404:timestamp:106177724","92404:timestamp:111076540"],"KeyTotalCnt":196}
93304:returnfootholdroom:[0-9a-z] : {"KeySamples":["93304:returnfootholdroom:1092146217992148","93304:returnfootholdroom:1921460801261432","93304:returnfootholdroom:1921460263814680"],"KeyTotalCnt":586}


