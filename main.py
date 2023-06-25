import pandas as pd 
import psycopg2
import requests

df = pd.read_csv("dummyData/MapinkoopdataNL.csv")

def retrieveData(numberPlate):
    return df.loc[df['KENTEKEN'] == numberPlate]


def retrieveDataFromApi(numberPlate):
    url = "https://api.rdw.nl/ovi-a/version=1"
    body = {"KENTEKEN": numberPlate} 
    # Add the personal key in the headers field
    headers = {"Content-Type":"application/json","Ocp-Apim-Subscription-Key":"THE_PERSONAL_KEY"}
    response = requests.post(url,json=body,headers=headers)
    return response.json()



def connectToDataBase():
    hostname    = "localhost"
    database    = "carData"
    username    = "postgres"
    pwd         = "admin"
    port_id     = "5432"
    connection = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    return connection

def saveToDatabase(data):
    connection = connectToDataBase()
    cursor = connection.cursor()
    query = ''' INSERT INTO marketData (Voertuigkenmerk, Voertuigprijs,Kilometerstand,Typevoertuig,Bouwjaar,Onderhoudsstatus,Landafkomst,Voertuigmodel,Voertuigafskomt,Datum van waarde) 
                VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)'''
    dataToSave = (data['Voertuigkenmerk'].values[0],
                  data['Voertuigprijs'].values[0],
                  data['Kilometerstand'].values[0],
                  data['Typevoertuig'].values[0],
                  data['Bouwjaar'].values[0],
                  data['Onderhoudsstatus'].values[0],
                  data['Landafkomst'].values[0],
                  data['Voertuigmodel'].values[0],
                  data['Voertuigafskomt'].values[0],
                  data['Datum van waarde'].values[0])
    try:
    # Execute the SQL query
        cursor.execute(query, data)

        # Commit the transaction
        connection.commit()
        print("Data inserted successfully!")

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error inserting data:", error)

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

def run():
    # connectToDataBase()
    print(''' 

    ******************************************************************************
    ******************** Welcome to the car-data retriever ***********************
    ******************************************************************************\n
    ''')
    print("Do want to use the API? [Y/'N]")
    answer = input().upper()
    while(answer != "Y" and answer != "N"):
        print("Invalid answer! Please answer with Y or N")
        answer = input()
    numberPlate : str = input("To retrieve car-data and save it to the postgres database, please enter a number plate:\nNumber plate: ")
    while (len(numberPlate) != 6):
        print("You have inputted an invalid number plate, the number plate should be 6 characters. Please try again. ")
        numberPlate = input(("Number plate: "))
    numberPlate = numberPlate.upper()

    if(answer.upper() == "Y"):
        print(retrieveDataFromApi(numberPlate))
    else:
        data = retrieveData(numberPlate)
        print(data)
        print("Do you want to save the data to database? [Y/N]")
        answer = input().upper()
        while(answer != "Y" and answer != "N"):
            print("Invalid answer! Please answer with Y or N")
            answer = input()
        if (answer == 'Y'):
            saveToDatabase(data)
        else:
            print("No data was saved! Exiting the program.")




if __name__ == "__main__":
    run()