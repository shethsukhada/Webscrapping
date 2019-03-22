import requests
from bs4 import BeautifulSoup
import psycopg2
from congif import config
from re import sub
    

def connect(data):
    """ Connect to the PostgreSQL database server """
    conn = None

    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO real_estate(address,locality,area,beds,full_bath,half_bath,price,lot_size)
             VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""


    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
 
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        address, locality, price, beds, sqFt,halfBath, fullBath, lotSize, flag = None , None ,None , None , None , None , None ,None , None 

        
        for record in data:
            
            address = record['Address']
            locality = record['Locality']
            if not record['Price'] is None :
                price = record['Price']                
                price = int(sub(r'[^\d.]', '', price))
    

            if not record['Beds'] is None :
                beds = record['Beds']

            if not record['SqFt'] is None :
                sqFt = record['SqFt']
                sqFt = int(sqFt.replace(',',''))
            

            if not record['HalfBath'] is None :
                halfBath = record['HalfBath']

            if not record['FullBath'] is None :
                fullBath = record['FullBath']
            
            try:
                if not record['LotSize'] is None :
                    lotSize = record['LotSize']
            except:
                lotSize = ''

            cur.execute(sql, (address,locality,sqFt,beds,fullBath,halfBath,price,lotSize))

            id = cur.fetchone()[0]

            if id is None:
                flag = False

            print(id)

        if flag is not False:
            conn.commit()

       
     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 

def scrape():
    l=[]
    base_url="https://pythonhow.com/real-estate/rock-springs-wy/LCWYROCKSPRINGS/t=0&s="



    for page in range(0,30,10):
        currentURL = base_url+str(page)+".html"

        r = requests.get(currentURL)
        c = r.content

        soup = BeautifulSoup(c,"html.parser")
        #print(soup.prettify())

        all = soup.find_all("div",{"class":"propertyRow"})

    
        for item in all:
            d={}
            d["Address"]=item.find_all("span",{"class":"propAddressCollapse"})[0].text
            d["Locality"]=item.find_all("span",{"class":"propAddressCollapse"})[1].text
            d["Price"]=item.find("h4",{"class":"propPrice"}).text.replace("\n","").replace(" ","")

            try:
                d["Beds"]=item.find("span",{"class":"infoBed"}).find("b").text
            except:
                d["Beds"]=None

            try:
                d["SqFt"]=item.find("span",{"class":"infoSqFt"}).find("b").text
            except:
                d["SqFt"]= None

            try:
                d["FullBath"]=item.find("span",{"class":"infoValueFullBath"}).find("b").text
            except:
                d["FullBath"]=None
            
            try:
                d["HalfBath"]=item.find("span",{"class":"infoValueHalfBath"}).find("b").text
            except:
                d["HalfBath"]=None

            for column_group in item.find_all("div", {"class":"columnGroup"}):
                for feature_group , feature_name in zip(column_group.find_all("span",{"class":"featureGroup"}) , column_group.find_all("span",{"class":"featureName"})):
                    #print(feature_group.text, "   ", feature_name.text)
                    if "Lot Size" in feature_group.text:
                        d["LotSize"]=feature_name.text
        
            l.append(d)

        import pandas

    df=pandas.DataFrame(l)

    print(df)

    df.to_csv("Output.csv")

    connect(l)

            
 
if __name__ == '__main__':
    
    scrape()


