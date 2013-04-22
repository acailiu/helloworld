import time
import datetime
import logging
import pylog


logger = logging.getLogger()
CONF_BASE_PATH = "/data/Walrus/conf/"
MAX_FRE = 100

# flag: (tablename,filename,desc,delims)
cfg_list = {
    "gender":   ("t_gender", "uds_gender.cfg", "gender,gender_description", "\t"),
    "coverid":    ("t_cover", "coverinfo_yyyymmdd.txt", "coverid,cover_chinese", "\t"),
    "loc_code":    ("t_loc", "locinfo_yyyymmdd.cfg", "loc_code,loc_id,loc_chi", "\t"),
    "vid":        ("t_vid", "qqlive_vad_yyyymmdd.cfg", "vid,content_id,vtype,vsubtype,vurl,vname,vtimes,vsubprogram", "\t"),
    "vtype":    ("t_vtype", "typeinfo.txt", "vtype,vtype_chi", "\t"),
    "area":        ("t_area", "area_info_yyyymmdd.cfg", "area,area_desc,area_type,area_country,area_prov,area_cap_tag,area_keycity_tag", "\t"),
    "content_id":    ("t_content_id", "video_contentinfo_yyyymmdd.cfg", "content_id,content_chi", "\t"),
    "client_id":    ("t_client_id", "clientinfo_yyyymmdd.cfg", "client_id,client_id_chi,client_id_short", "\t")
}

d_info = {}


def load_all(result_desc):
    """Load yesterday conf.
    """
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y%m%d")
    r_field = result_desc.split(',')
    r_field = [i.strip() for i in r_field]
    for key in cfg_list:
        if key not in r_field:
            continue
        d_info[key] = {}
        tableName, fileName, desc, delims = cfg_list[key]
        len_desc = len(desc.split(','))
        # load conf
        fileName = fileName.replace('yyyymmdd', yesterday)
        fileUrl = "%s/%s" % (CONF_BASE_PATH, fileName)
        try:
            for line in file(fileUrl).readlines():
                sTemp = line.strip('\r\n').split(delims)
                if len(sTemp) != len_desc:
                    continue
                d_info[key][sTemp[0]] = ",".join(sTemp[0:len_desc])
            d_info[key]["unknown"] = ["unknown" for i in xrange(len_desc-1)]
            d_info[key]["unknown"] = ",".join(d_info[key]["unknown"])
        except Exception, ex:
            logger.error("Load %s data failed! (%s)" % (fileUrl, str(ex)))


def getNextDay(today):
    date = time.strptime(today, "%Y%m%d")
    tomorrow = datetime.datetime(date[0], date[1], date[2]) + datetime.timedelta(days=1)
    tomorrow = tomorrow.strftime("%Y%m%d")
    return tomorrow


def make_add_uv_excel(result_desc, input_url, output_url):
    """ oid,area,date,fre,count
    """
    area_code = {}
    area_flag = False
    if "area" in result_desc:
        area_flag = True
        area_index = result_desc.split(',').index('area')
        f_area = file('city_code.cfg', 'r')
        while True:
            line = f_area.readline()
            if line:
                line = line.strip('\r\n')
                sTemp = line.split('\t')
                code = sTemp[0].split(',')
                area = sTemp[1]
                try:
                    a = int(code[0])
                    b = int(code[1])
                    if code[2] == "":
                        c = 0
                    else:
                        c = int(code[2])
                    code = 256*256*a + 256*b + c
                except:
                    code = 0
                try:
                    area_code[code] = pylog.to_gbk(area)
                except:
                    area_code[code] = "unknown"
            else:
                break

    f_input = file(input_url, "r")
    if not f_input:
        raise Exception("Make add uv excel failed! Input file %s dosen't exist!" % input_url)
    g_result = {}
    groupby_num = len(result_desc.split(",")) - 3
    if groupby_num <= 0:
        raise Exception("Wrong result_desc %s!" % result_desc)
    while True:
        line = f_input.readline()
        if line:
            line = line.strip("\r\n")
            sTemp = line.split("\t")
            key = ",".join(sTemp[0:groupby_num])
            if key not in g_result:
                g_result[key] = {}
            day = sTemp[groupby_num]
            fre = int(sTemp[groupby_num + 1])
            count = int(sTemp[groupby_num + 2])
            if day not in g_result[key]:
                g_result[key][day] = []
                for i in xrange(20):
                    g_result[key][day].append(0)
            g_result[key][day][fre - 1] = count
        else:
            break

    # output
    f_output = file(output_url, "w")
    if not f_output:
        raise Exception("Make output excel failed! Output file %s!" % output_url)
    a = ",".join(result_desc.split(",")[0:groupby_num + 1])
    b = ",".join([str(i + 1) for i in xrange(19)])
    b += ",20+"
    f_output.write("%s,%s\n" % (a, b))
    for key in g_result:
        day_result_all = sorted(g_result[key].items())
        if area_flag:
            sTemp = key.split(',')
            if sTemp[area_index] != "":
                code = int(sTemp[area_index])
            else:
                code = 0
            if code in area_code:
                sTemp[area_index] = area_code[code]
            else:
                sTemp[area_index] = "unknown"
            key = ",".join(sTemp)
        for day_result in day_result_all:
            day = day_result[0]
            result = day_result[1]

            wTemp = key + "," + day + "," + ",".join([str(i) for i in result]) + "\n"
            f_output.write(wTemp)


def make_puv_excel(result_desc, input_url, output_url):
    """key,pv,uv
    """
    load_all(result_desc)
    f_input = file(input_url, "r")
    if not f_input:
        raise Exception("Make puv excel failed! Input file %s dosen't exist!" % input_url)
    r_field = result_desc.split(',')
    if "money" in r_field:
        keyNum = len(r_field) - 3
        moneyFlag = True
    else:
        keyNum = len(r_field) - 2
        moneyFlag = False
    if keyNum < 0:
        raise Exception("Wrong result_desc %s!" % result_desc)

    # read input
    d_result = {}
    for line in f_input.readlines():
        sTemp = line.strip('\r\n').split('\t')
        if keyNum == 0:
            key = "all"
        else:
            for i in xrange(keyNum):
                field = r_field[i].strip()
                if field in d_info:
                    if sTemp[i] in d_info[field]:
                        sTemp[i] = d_info[field][sTemp[i]]
                    else:
                        sTemp[i] = sTemp[i] + "," + d_info[field]["unknown"]
            key = ",".join(sTemp[0:keyNum])
        if moneyFlag:
            pv, uv, money = tuple(sTemp[keyNum:])
        else:
            pv, uv = tuple(sTemp[keyNum:])
        if key not in d_result:
            # init pv,uv
            d_result[key] = {}
            d_result[key]["pv"] = 0
            d_result[key]["uv"] = 0
            if moneyFlag:
                d_result[key]["money"] = 0
        d_result[key]["pv"] += int(pv)
        d_result[key]["uv"] += int(uv)
        if moneyFlag:
            try:
                d_result[key]["money"] += float(money)
            except:
                pass
    f_input.close()

    # output
    f_output = file(output_url, "w")
    if not f_output:
        raise Exception("Make output excel failed! Output file %s!" % output_url)
    for i in xrange(keyNum):
        if r_field[i] in cfg_list:
            r_field[i] = cfg_list[r_field[i]][2]
    nav_row = ",".join(r_field[0:keyNum])
    if nav_row == "":
        nav_row = "all"
    if moneyFlag:
        f_output.write("%s,pv,uv,money\n" % nav_row)
    else:
        f_output.write("%s,pv,uv\n" % nav_row)
    for key in d_result:
        if moneyFlag:
            wTemp = "%s,%d,%d,%f\n" % (key, d_result[key]["pv"], d_result[key]["uv"], d_result[key]["money"])
        else:
            wTemp = "%s,%d,%d\n" % (key, d_result[key]["pv"], d_result[key]["uv"])
        f_output.write(wTemp)
    f_output.close()


def make_freq_excel(result_desc, input_url, output_url):
    """key,fre,pv,uv
    """
    load_all(result_desc)
    f_input = file(input_url, "r")
    if not f_input:
        raise Exception("Make freq uv excel failed! Input file %s dosen't exist!" % input_url)
    r_field = result_desc.split(',')
    r_field = [i.strip() for i in r_field]
    keyNum = len(r_field) - 3
    if keyNum < 0:
        raise Exception("Wrong result_desc %s!" % result_desc)

    # read input
    d_result = {}
    for line in f_input.readlines():
        sTemp = line.strip('\r\n').split('\t')
        if keyNum == 0:
            key = "all"
        else:
            for i in xrange(keyNum):
                field = r_field[i].strip()
                if field in d_info:
                    if sTemp[i] in d_info[field]:
                        sTemp[i] = d_info[field][sTemp[i]]
                    else:
                        sTemp[i] = sTemp[i] + "," + d_info[field]["unknown"]
            key = ",".join(sTemp[0:keyNum])
        fre, pv, uv = tuple(sTemp[keyNum:])
        try:
            fre = int(fre)
            pv = int(pv)
            uv = int(uv)
        except:
            continue
        if fre > MAX_FRE:
            fre = MAX_FRE
        if key not in d_result:
            # init pv,uv
            d_result[key] = {}
            d_result[key]["fre"] = [0 for i in xrange(MAX_FRE)]
            d_result[key]["pv"] = 0
            d_result[key]["uv"] = 0
        d_result[key]["fre"][fre-1] += uv
        d_result[key]["pv"] += pv
        d_result[key]["uv"] += uv
    f_input.close()

    # output
    f_output = file(output_url, "w")
    if not f_output:
        raise Exception("Make output excel failed! Output file %s!" % output_url)
    for i in xrange(keyNum):
        if r_field[i] in cfg_list:
            r_field[i] = cfg_list[r_field[i]][2]
    import sys
    nav_row = ",".join(r_field[0:keyNum])
    if nav_row == "":
        nav_row = "all"
    f_output.write("%s,pv,uv,%s,%d+\n" % (nav_row, ",".join([str(i+1) for i in xrange(MAX_FRE-1)]), MAX_FRE))
    for key in d_result:
        wTemp = "%s,%d,%d,%s\n" % (key, d_result[key]["pv"], d_result[
                                   key]["uv"], ",".join([str(i) for i in d_result[key]["fre"]]))
        f_output.write(wTemp)
    f_output.close()

