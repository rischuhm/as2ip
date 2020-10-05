import requests, re, datetime, gzip,json, os
import pandas as pd
import ipaddress

cwd = os.getcwd()
    
def get_file_url():
    x = datetime.datetime.now()
    month = x.strftime("%m")
    base_url = f"http://data.caida.org/datasets/routing/routeviews-prefix2as/{x.year}/{month}/"

    r = requests.get(base_url)
    pattern = r'routeviews.rv2-\d*-\d*.pfx2as.gz'

    return base_url+re.findall(pattern,r.text)[-1]

def get_file(file_url):
    with open(f'{cwd}/file.gz', 'wb') as f:
        r = requests.get(file_url)
        f.write(r.content)

def unzip_file():
    f = gzip.open(f'{cwd}/file.gz', 'rb')
    file_content = f.read()
    data = open(f'{cwd}/data.csv', 'wb')
    data.write(file_content)
    data.close()

def build_file():
    pd.set_option('display.max_rows', None)
    print(f"""{"x"*50}\nLoading file\n{"x"*50}\n""")
    df = pd.read_csv(f'{cwd}/data.csv',sep='\t',names=["IP","Subnet","ASnr"])

    print(f"""{"x"*50}\nBuilding nets\n{"x"*50}\n""")
    df["Nets"] = df["IP"] + "/" + df["Subnet"].astype(str)


    ##########################################################
    #################TESTNETZE################################
    #df["ASnr"] == "") 56203, 48951, 23294
    df = df[["Subnet","Nets","ASnr"]].sort_values(by="Subnet")
    #df = df[["Subnet","Nets","ASnr"]][(df["ASnr"] == "48951") | (df["ASnr"] == "56203") | df['ASnr'].str.contains("_")  ].sort_values(by="Subnet")
    #df = df[["Subnet","Nets","ASnr"]][df['ASnr'].str.contains("_")]
    ##########################################################
    #########################################################

    print(f"""{"x"*50}\nCleaning data\n{"x"*50}\n""")

    for ind in df.index:
        if "_" in str(df["ASnr"][ind]):
            #print(df["ASnr"][ind])
            pattern = r"\d+"
            string = str(df["ASnr"][ind])
            # print(re.findall(pattern, string)[0])
            df.at[ind, "ASnr"] = re.findall(pattern, string)[0]

    print(f"""{"x"*50}\nBuilding JSON\n{"x"*50}\n""")

    set_list = []
    asnr_list = []
    for ind in df.index:
        asnr =  df["ASnr"][ind]
        subnet =  df["Nets"][ind]
        if asnr not in asnr_list:
            asnr_list.append(asnr)
            print(f"""+ Processed: {round(len(asnr_list)/len(df["ASnr"].unique())*100,1)}%""",end="\r")
            mydata = {"ASNR":asnr,"NETS":[subnet],"IN":0,"OUT":0}
            set_list.append(mydata)
        else:
            for item in set_list:
                if item["ASNR"] == asnr:
                    for i in range(len(item["NETS"])):
                        if (ipaddress.ip_network(subnet).subnet_of(ipaddress.ip_network(item["NETS"][i]))):
                            break
                        elif item["NETS"][-1] and not(ipaddress.ip_network(subnet).subnet_of(ipaddress.ip_network(item["NETS"][-1]))):
                            item["NETS"].append(subnet) 
                

    print("100.00%")
    print(f"""{"x"*50}\nFinished building JSON. Writing the file\n{"x"*50}\n""")
            
    data = json.dumps(set_list)
    #print(data)
    with open(f"{cwd}/as2ip.json","w") as f:
        f.write(data)

    print(f"""{"x"*50}\nWrote inventory file.\nCompletely done. \n{"x"*50}\n""")


# get_file(get_file_url())
# unzip_file()
build_file()