import streamlit as st
from streamlit_option_menu import option_menu
import pymysql
import pandas as pd
import plotly.express as px
import json
import requests
from PIL import Image
import random

connection = pymysql.connect(
        host='localhost',
        user='root',
        password='DataScience@12',
        database="myphonepe",
        port=3306
    )
mycursor=connection.cursor()

# Converting MySQL Database to pandas Dataframe

#Aggregated_insurance
mycursor.execute("Select *from aggregated_insurance")
connection.commit()
table1=mycursor.fetchall()
Aggre_insurance=pd.DataFrame(table1, columns=("States", "Year", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

#Aggregated_transaction
mycursor.execute("Select *from aggregated_transaction")
connection.commit()
table2=mycursor.fetchall()
Aggre_transaction=pd.DataFrame(table2, columns=("States", "Year", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

#Aggregated_user
mycursor.execute("Select *from aggregated_user")
connection.commit()
table3=mycursor.fetchall()
Aggre_user=pd.DataFrame(table3, columns=("States", "Year", "Quarter", "Brands", "Transaction_count", "Percentage"))

#Map_insurance
mycursor.execute("Select *from map_insurance")
connection.commit()
table4=mycursor.fetchall()
Map_insurance=pd.DataFrame(table4, columns=("States", "Year", "Quarter", "District", "Transaction_count", "Transaction_amount"))

#Map_transaction
mycursor.execute("Select *from map_transaction")
connection.commit()
table5=mycursor.fetchall()
Map_transaction=pd.DataFrame(table5, columns=("States", "Year", "Quarter", "District", "Transaction_count", "Transaction_amount"))

#Map_user
mycursor.execute("Select *from map_user")
connection.commit()
table6=mycursor.fetchall()
Map_user=pd.DataFrame(table6, columns=("States", "Year", "Quarter", "District", "Registered_users", "App_opens"))

#Top_insurance
mycursor.execute("Select *from top_insurance")
connection.commit()
table7=mycursor.fetchall()
Top_insurance=pd.DataFrame(table7, columns=("States", "Year", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))

#Top_transaction
mycursor.execute("Select *from top_transaction")
connection.commit()
table8=mycursor.fetchall()
Top_transaction=pd.DataFrame(table8, columns=("States", "Year", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))

#Top_user
mycursor.execute("Select *from top_user")
connection.commit()
table9=mycursor.fetchall()
Top_user=pd.DataFrame(table9, columns=("States", "Year", "Quarter", "Pincodes", "Registered_users"))

#Query part
def Top_chart_transaction_amount(table_name):
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='DataScience@12',
            database="myphonepe",
            port=3306
        )
    mycursor=connection.cursor()
    #plot_1
    query1=f'''SELECT states, sum(Transaction_amount) AS Transaction_amount
                from {table_name} GROUP BY states ORDER BY Transaction_amount DESC Limit 10;'''
    mycursor.execute(query1)
    table_1=mycursor.fetchall()
    connection.commit()

    df_1=pd.DataFrame(table_1, columns= ("States", "Transaction_amount"))

    fig_amount_1=px.bar(df_1, x="States", y="Transaction_amount", title= "TRANSACTION AMOUNT", hover_name="States",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
    st.plotly_chart(fig_amount_1)

    #plot_2
    query2=f'''SELECT states, sum(Transaction_amount) AS Transaction_amount
                from {table_name} GROUP BY states ORDER BY Transaction_amount Limit 10;'''
    mycursor.execute(query2)
    table_2=mycursor.fetchall()
    connection.commit()

    df_2=pd.DataFrame(table_2, columns= ("States", "Transaction_amount"))

    fig_amount_2=px.bar(df_2, x="States", y="Transaction_amount", title= "TRANSACTION AMOUNT", hover_name="States",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650, width=600)
    st.plotly_chart(fig_amount_2)

    query3=f'''SELECT states, avg(Transaction_amount) AS Transaction_amount
                from {table_name} GROUP BY states ORDER BY Transaction_amount;'''
    mycursor.execute(query3)
    table_3=mycursor.fetchall()
    connection.commit()

    df_3=pd.DataFrame(table_3, columns= ("States", "Transaction_amount"))

    fig_amount_3=px.bar(df_3, y="States", x="Transaction_amount", title= "TRANSACTION AMOUNT", hover_name="States", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)

def Top_chart_transaction_count(table_name):
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='DataScience@12',
            database="myphonepe",
            port=3306
        )
    mycursor=connection.cursor()
    #plot_1
    query1=f'''SELECT states, sum(Transaction_count) AS Transaction_count
                from {table_name} GROUP BY states ORDER BY Transaction_count DESC Limit 10;'''
    mycursor.execute(query1)
    table_1=mycursor.fetchall()
    connection.commit()

    df_1=pd.DataFrame(table_1, columns= ("States", "Transaction_count"))

    fig_amount_1=px.bar(df_1, x="States", y="Transaction_count", title= "TRANSACTION COUNT", hover_name="States",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
    st.plotly_chart(fig_amount_1)

    #plot_2
    query2=f'''SELECT states, sum(Transaction_count) AS Transaction_count
                from {table_name} GROUP BY states ORDER BY Transaction_count Limit 10;'''
    mycursor.execute(query2)
    table_2=mycursor.fetchall()
    connection.commit()

    df_2=pd.DataFrame(table_2, columns= ("States", "Transaction_count"))

    fig_amount_2=px.bar(df_2, x="States", y="Transaction_count", title= "TRANSACTION COUNT", hover_name="States",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650, width=600)
    st.plotly_chart(fig_amount_2)

    query3=f'''SELECT states, avg(Transaction_count) AS Transaction_count
                from {table_name} GROUP BY states ORDER BY Transaction_count;'''
    mycursor.execute(query3)
    table_3=mycursor.fetchall()
    connection.commit()

    df_3=pd.DataFrame(table_3, columns= ("States", "Transaction_count"))

    fig_amount_3=px.bar(df_3, y="States", x="Transaction_count", title= "TRANSACTION COUNT", hover_name="States", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)

def Top_chart_registered_user(table_name, state):
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='DataScience@12',
            database="myphonepe",
            port=3306
        )
    mycursor=connection.cursor()
    #plot_1
    query1=f'''SELECT District_name, sum(Registered_users) AS Registered_users
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY Registered_users DESC Limit 10;'''
    mycursor.execute(query1)
    table_1=mycursor.fetchall()
    connection.commit()

    df_1=pd.DataFrame(table_1, columns= ("District_name", "Registered_users"))

    fig_amount_1=px.bar(df_1, x="District_name", y="Registered_users", title= "TOP 10 OF REGISTERED USER", hover_name="District_name",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
    st.plotly_chart(fig_amount_1)

    #plot_2
    query2=f'''SELECT District_name, sum(Registered_users) AS Registered_users
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY Registered_users Limit 10;'''
    mycursor.execute(query2)
    table_2=mycursor.fetchall()
    connection.commit()

    df_2=pd.DataFrame(table_2, columns= ("District_name", "Registered_users"))

    fig_amount_2=px.bar(df_2, x="District_name", y="Registered_users", title= "LAST 10 OF REGISTERED USER", hover_name="District_name",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650, width=600)
    st.plotly_chart(fig_amount_2)

    query3=f'''SELECT District_name, avg(Registered_users) AS Registered_users
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY Registered_users;'''
    mycursor.execute(query3)
    table_3=mycursor.fetchall()
    connection.commit()

    df_3=pd.DataFrame(table_3, columns= ("District_name", "Registered_users"))

    fig_amount_3=px.bar(df_3, y="District_name", x="Registered_users", title= "AVERAGE OF REGISTERED USER", hover_name="District_name", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)

def Top_chart_appopens(table_name, state):
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='DataScience@12',
            database="myphonepe",
            port=3306
        )
    mycursor=connection.cursor()
    #plot_1
    query1=f'''SELECT District_name, sum(App_opens) AS App_opens
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY App_opens DESC Limit 10;'''
    mycursor.execute(query1)
    table_1=mycursor.fetchall()
    connection.commit()

    df_1=pd.DataFrame(table_1, columns= ("District_name", "App_opens"))

    fig_amount_1=px.bar(df_1, x="District_name", y="App_opens", title= "TOP 10 OF APPOPENS", hover_name="District_name",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
    st.plotly_chart(fig_amount_1)

    #plot_2
    query2=f'''SELECT District_name, sum(App_opens) AS App_opens
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY App_opens Limit 10;'''
    mycursor.execute(query2)
    table_2=mycursor.fetchall()
    connection.commit()

    df_2=pd.DataFrame(table_2, columns= ("District_name", "App_opens"))

    fig_amount_2=px.bar(df_2, x="District_name", y="App_opens", title= "LAST 10 OF APPOPENS", hover_name="District_name",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650, width=600)
    st.plotly_chart(fig_amount_2)

    query3=f'''SELECT District_name, avg(App_opens) AS App_opens
                from {table_name} WHERE states='{state}' GROUP BY District_name ORDER BY App_opens;'''
    mycursor.execute(query3)
    table_3=mycursor.fetchall()
    connection.commit()

    df_3=pd.DataFrame(table_3, columns= ("District_name", "App_opens"))

    fig_amount_3=px.bar(df_3, y="District_name", x="App_opens", title= "AVERAGE OF APPOPENS", hover_name="District_name", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)

def Top_chart_registered_users(table_name):
    connection = pymysql.connect(
            host='localhost',
            user='root',
            password='DataScience@12',
            database="myphonepe",
            port=3306
        )
    mycursor=connection.cursor()
    #plot_1
    query1=f'''SELECT states, sum(Registered_users) AS Registered_users
                from {table_name}  GROUP BY states ORDER BY Registered_users DESC Limit 10;'''
    mycursor.execute(query1)
    table_1=mycursor.fetchall()
    connection.commit()

    df_1=pd.DataFrame(table_1, columns= ("states", "Registered_users"))

    fig_amount_1=px.bar(df_1, x="states", y="Registered_users", title= "TOP 10 OF REGISTERED USERS", hover_name="states",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
    st.plotly_chart(fig_amount_1)

    #plot_2
    query2=f'''SELECT states, sum(Registered_users) AS Registered_users
                from {table_name}  GROUP BY states ORDER BY Registered_users Limit 10;'''
    mycursor.execute(query2)
    table_2=mycursor.fetchall()
    connection.commit()

    df_2=pd.DataFrame(table_2, columns= ("states", "Registered_users"))

    fig_amount_2=px.bar(df_2, x="states", y="Registered_users", title= "LAST 10 OF REGISTERED USERS", hover_name="states",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650, width=600)
    st.plotly_chart(fig_amount_2)

    query3=f'''SELECT states, avg(Registered_users) AS Registered_users
                from {table_name}  GROUP BY states ORDER BY Registered_users;'''
    mycursor.execute(query3)
    table_3=mycursor.fetchall()
    connection.commit()

    df_3=pd.DataFrame(table_3, columns= ("states", "Registered_users"))

    fig_amount_3=px.bar(df_3, y="states", x="Registered_users", title= "AVERAGE OF REGISTERED USER", hover_name="states", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Magenta_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)

# Funtion defining for geo visualisation

def Transaction_amount_count_Y(df,year):
    tacy=df[df["Year"]==year]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(tacyg, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r, height=800, width=800)
        st.plotly_chart(fig_amount, key=f"fig_amount_{random.randint(0,10000)}")
    with col2:

        fig_count=px.bar(tacyg, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=800)
        st.plotly_chart(fig_count,key=f"fig_count_{random.randint(0,10000)}")
    
    col1,col2=st.columns(2)
    with col1:

        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(url)
        data1= json.loads(response.content)
        states_name=[]
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()


        fig_india_1=px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color="Transaction_amount", color_continuous_scale= "Inferno",
                                range_color=(tacyg["Transaction_amount"].min(),tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 500, width= 650,projection= "mercator")
        
        
        fig_india_1.update_geos(visible= False)
        st.plotly_chart(fig_india_1,key=f"fig_india_1{random.randint(0,10000)}") 
        
    with col2:

        fig_india_2=px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color="Transaction_count", color_continuous_scale= "Inferno",
                                range_color=(tacyg["Transaction_count"].min(),tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{year} TRANSACTION COUNT", fitbounds= "locations",
                                height= 500, width= 650,projection= "mercator")
        
        
        fig_india_2.update_geos(visible= False)
        st.plotly_chart(fig_india_2, key=f"fig_india_2{random.randint(0,10000)}")
        
    return tacy

def Transaction_amount_count_Y_Q(df,quarter):
    tacy=df[df["Quarter"]==quarter]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)
    
    col1,col2=st.columns(2)
    with col1:

        fig_amount=px.bar(tacyg, x="States", y="Transaction_amount", title=f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Agsunset_r,height=650, width=600)
        st.plotly_chart(fig_amount, key=f"fig_amount_{random.randint(0,10000)}")

    with col2:

        fig_count=px.bar(tacyg, x="States", y="Transaction_count", title=f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Bluered_r,height=650, width=600)
        st.plotly_chart(fig_count, key=f"fig_count_{random.randint(0,10000)}")
    
    col1,col2=st.columns(2)

    with col1:

        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(url)
        data1= json.loads(response.content)
        states_name=[]
        for feature in data1["features"]:
            states_name.append(feature["properties"]["ST_NM"])

        states_name.sort()

    

        fig_india_1=px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color="Transaction_amount", color_continuous_scale= "Inferno",
                                range_color=(tacyg["Transaction_amount"].min(),tacyg["Transaction_amount"].max()),
                                hover_name= "States", title= f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION AMOUNT", fitbounds= "locations",
                                height= 500, width= 650,projection= "mercator")
        
        
        fig_india_1.update_geos(visible= False)
        st.plotly_chart(fig_india_1, key=f"fig_india_1{random.randint(0,10000)}")
        
    with col2:

        fig_india_2=px.choropleth(tacyg, geojson= data1, locations= "States", featureidkey= "properties.ST_NM",
                                color="Transaction_count", color_continuous_scale= "Inferno",
                                range_color=(tacyg["Transaction_count"].min(),tacyg["Transaction_count"].max()),
                                hover_name= "States", title= f"{tacy['Year'].min()} YEAR {quarter} QUARTER TRANSACTION COUNT", fitbounds= "locations",
                                height= 500, width= 650,projection= "mercator")
        
        
        fig_india_2.update_geos(visible= False)
        st.plotly_chart(fig_india_2, key=f"fig_india_2{random.randint(0,10000)}")
        
    return tacy

def Aggre_Trans_type(df, state):
    tacy=df[df["States"]==state]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    col1,col2=st.columns(2)

    with col1:
    
        fig_pie_1=px.pie(data_frame=tacyg, names="Transaction_type", values="Transaction_amount",
                        width=600, title=f"{state.upper()} TRANSACTION AMOUNT", hole=0.5)
        st.plotly_chart(fig_pie_1)
    with col2:
        fig_pie_2=px.pie(data_frame=tacyg, names="Transaction_type", values="Transaction_count",
                        width=600, title=f"{state.upper()} TRANSACTION COUNT", hole=0.5)
        st.plotly_chart(fig_pie_2)

def aggre_user_plot_Y(df, year):
    aguy=df[df["Year"]==year]
    aguy.reset_index(drop=True, inplace=True)

    aguyg=pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace=True)

    fig_bar_1=px.bar(aguyg, x="Brands", y= "Transaction_count", title=f"{year} BRANDS AND TRANSACTION", 
                    width=800, color_discrete_sequence=px.colors.sequential.matter, hover_name= "Brands")
    st.plotly_chart(fig_bar_1)
    return aguy

def aggre_user_plot_Q(df, quarter):
    aguyq=df[df["Quarter"]==quarter]
    aguyq.reset_index(drop=True, inplace=True)

    aguyqg=pd.DataFrame(aguyq.groupby("Brands")["Transaction_count"].sum())
    aguyqg.reset_index(inplace=True)

    fig_bar_1=px.bar(aguyqg, x="Brands", y= "Transaction_count", title=f"{quarter} QUARTER, BRANDS AND TRANSACTION", 
                    width=800, color_discrete_sequence=px.colors.sequential.matter, hover_name= "Brands")
    st.plotly_chart(fig_bar_1)
    return aguyq

def aggre_user_plot_S(df, state):
    aguyqs=df[df["States"]==state]
    aguyqs.reset_index(drop=True, inplace=True)

    fig_line_1=px.line(aguyqs, x="Brands", y= "Transaction_count",hover_data= "Percentage", title= f"{state.upper()} BRANDS, TRANSACTION_COUNT AND PERCENTAGE", 
                    width=800,markers=True)
    st.plotly_chart(fig_line_1)

def Map_insur_District(df,state):
    tacy=df[df["States"]==state]
    tacy.reset_index(drop=True, inplace=True)

    tacyg=tacy.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)
    
    col1,col2=st.columns(2)

    with col1:
        fig_bar_1=px.bar(tacyg, x="Transaction_amount", y="District", orientation="h", height=600,
                        title=f"{state.upper()} DISTRICT AND TRANSACTION AMOUNT", color_discrete_sequence= px.colors.sequential.Viridis)
        st.plotly_chart(fig_bar_1)

    with col2:
        fig_bar_2=px.bar(tacyg, x="Transaction_amount", y="District", orientation="h", height=600,
                        title=f"{state.upper()} DISTRICT AND TRANSACTION COUNT", color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_bar_2)

def map_user_plot_1(df,year):
    muy=df[df["Year"]==year]
    muy.reset_index(drop=True, inplace=True)

    muyg=muy.groupby("States")[["Registered_users", "App_opens"]].sum()
    muyg.reset_index(inplace=True)

    fig_line_1=px.line(muyg, x="States", y= ["Registered_users","App_opens"], 
                       title= f"{year} REGISTERED USER, APPOPENS", width=800, height= 800, markers=True)
    st.plotly_chart(fig_line_1)
    return muy

def map_user_plot_2(df,quarter):
    muyq=df[df["Quarter"]==quarter]
    muyq.reset_index(drop=True, inplace=True)

    muyqg=muyq.groupby("States")[["Registered_users", "App_opens"]].sum()
    muyqg.reset_index(inplace=True)

    fig_line_1=px.line(muyqg, x="States", y= ["Registered_users","App_opens"], 
                       title= f"{quarter} QUARTER REGISTERED USER, APPOPENS", width=800, height= 800, markers=True,
                       color_discrete_sequence=px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_line_1)
    return muyq

def map_user_plot_3(df, state):
    muyqs=df[df["States"]==state]
    muyqs.reset_index(drop=True, inplace=True)
    
    col1,col2=st.columns(2)
    with col1:
        fig_bar_1=px.bar(muyqs, x="Registered_users", y="District", orientation="h",
                        title="REGISTERED USER", height=800, color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(fig_bar_1)

    with col2:

        fig_bar_2=px.bar(muyqs, x="App_opens", y="District", orientation="h",
                        title="APPOPENS", height=800, color_discrete_sequence=px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_bar_2)

def Top_insur_plot_1(df, state):
    tiy=df[df["States"]==state]
    tiy.reset_index(drop=True, inplace=True)

    col1,col2=st.columns(2)
    with col1:

        fig_bar_1=px.bar(tiy, x="Quarter", y="Transaction_amount", hover_data="Pincodes",
                        title=f"{state.upper()} TRANSACTION AMOUNT", height=600, width=600, color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(fig_bar_1)

    with col2:
        fig_bar_2=px.bar(tiy, x="Quarter", y="Transaction_count", hover_data="Pincodes",
                        title=f"{state.upper()} TRANSACTION COUNT", height=600, width=600, color_discrete_sequence=px.colors.sequential.Bluered)
        st.plotly_chart(fig_bar_2)

def Top_user_plot_1(df, year):
    tuy=df[df["Year"]==year]
    tuy.reset_index(drop=True, inplace=True)

    tuyg=pd.DataFrame(tuy.groupby(["States", "Quarter"])["Registered_users"].sum())
    tuyg.reset_index(inplace=True)

    fig_bar_1=px.bar(tuyg, x="States", y="Registered_users", color="Quarter", hover_name="States",
                     title=f"{year} REGISTERED USER", height=800, width=800, color_discrete_sequence=px.colors.sequential.BuPu)
    st.plotly_chart(fig_bar_1)

    return tuy

def Top_user_plot_2(df,state):
    tuys=df[df["States"]==state]
    tuys.reset_index(drop=True, inplace=True)

    fig_bar_2=px.bar(tuys, x="Quarter",y="Registered_users", title= "REGISTERED USER, QUARTER AND PINCODES", hover_data="Pincodes",
                     width=800, height=800, color="Registered_users", color_continuous_scale=px.colors.sequential.Magma)
    st.plotly_chart(fig_bar_2)


#streamlit_part
st.set_page_config(layout= "wide", )
st.title(":violet[PHONE DATA VISUALISATION AND EXPLORATION]")

with st.sidebar:
    select= option_menu("Main Menu",["HOME", "DATA EXPLORATION", "TOP CHARTS"])

if select=="HOME":
    col1,col2= st.columns(2)
    
    with col1:
        st.header(":violet[PHONEPE]")
        st.subheader(":violet[INDIA'S BEST TRANSACTION APP]")
        st.markdown(":violet[PhonePe  is an Indian digital payments and financial technology company]")
        st.write("****:violet[FEATURES:]****")
        st.write("****~Credit & Debit card linking****")
        st.write("****~Bank Balance check****")
        st.write("****~Money Storage****")
        st.write("****~PIN Authorization****")
        st.download_button(":orange[DOWNLOAD THE APP NOW]", "https://www.phonepe.com/app-download/")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
        st.write("")
    
    with col2:
        st.image(Image.open(r"D:\project\phonepe image.jpg"))

elif select=="DATA EXPLORATION":
    tab1,tab2,tab3=st.tabs(["Aggregrated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:
        method=st.radio("Select The Method",["Insurance Analysis", "Transaction Analysis", "User Analysis"])

        if method=="Insurance Analysis":
            years=st.slider("Select the Year", Aggre_insurance["Year"].min(),Aggre_insurance["Year"].max(),Aggre_insurance["Year"].min())
            tac_Y= Transaction_amount_count_Y(Aggre_insurance,years)
            
            quarters=st.slider("Select the Quarter", tac_Y["Quarter"].min(),tac_Y["Quarter"].max(),tac_Y["Quarter"].min())
            tac_Y= Transaction_amount_count_Y_Q(tac_Y,quarters)

        elif method=="Transaction Analysis":
            years=st.slider("Select the Year_ta", Aggre_transaction["Year"].min(),Aggre_transaction["Year"].max(),Aggre_transaction["Year"].min())
            Aggre_trans_tac_Y=Transaction_amount_count_Y(Aggre_transaction,years)
            
            states=st.selectbox("Select The state",Aggre_trans_tac_Y["States"].unique())
            Aggre_Trans_type(Aggre_trans_tac_Y, states)

            quarters=st.slider("Select the Quarter", Aggre_trans_tac_Y["Quarter"].min(),Aggre_trans_tac_Y["Quarter"].max(),Aggre_trans_tac_Y["Quarter"].min())
            Aggre_trans_tac_Y_Q= Transaction_amount_count_Y_Q(Aggre_trans_tac_Y,quarters)

            states=st.selectbox("Select The state_ta",Aggre_trans_tac_Y_Q["States"].unique())
            Aggre_Trans_type(Aggre_trans_tac_Y_Q, states)

                                           
        elif method=="User Analysis":
            years=st.slider("Select the Year_ua", Aggre_user["Year"].min(),Aggre_user["Year"].max(),Aggre_user["Year"].min())
            aggre_user_Y=aggre_user_plot_Y(Aggre_user,years)

            quarters=st.slider("Select the Quarter_ua", aggre_user_Y["Quarter"].min(),aggre_user_Y["Quarter"].max(),aggre_user_Y["Quarter"].min())
            aggre_user_Y_Q= aggre_user_plot_Q(aggre_user_Y,quarters)

            states=st.selectbox("Select The state_ta",aggre_user_Y_Q["States"].unique())
            aggre_user_Y_Q_S= aggre_user_plot_S(aggre_user_Y_Q, states)

    with tab2:
        method2=st.radio("Select The Method",["Map Insurance", "Map Transaction", "Map User"])
        if method2=="Map Insurance":
            years=st.slider("**Select the Year_mi**", Map_insurance["Year"].min(),Map_insurance["Year"].max(),Map_insurance["Year"].min())
            map_insurance_tac_Y=Transaction_amount_count_Y(Map_insurance,years)

            states=st.selectbox("Select The state_mi",map_insurance_tac_Y["States"].unique())
            Map_insur_District(map_insurance_tac_Y, states)

            quarters=st.slider("Select the Quarter_mi", map_insurance_tac_Y["Quarter"].min(),map_insurance_tac_Y["Quarter"].max(),map_insurance_tac_Y["Quarter"].min())
            Map_insur_tac_Y_Q= Transaction_amount_count_Y_Q(map_insurance_tac_Y,quarters)

            states=st.selectbox("Select The state_ma",Map_insur_tac_Y_Q["States"].unique())
            Map_insur_District(Map_insur_tac_Y_Q, states)

        elif method2=="Map Transaction":
            years=st.slider("Select the Year_mt", Map_transaction["Year"].min(),Map_transaction["Year"].max(),Map_transaction["Year"].min())
            Map_trans_tac_Y=Transaction_amount_count_Y(Map_transaction,years)

            states=st.selectbox("Select The state_mt",Map_trans_tac_Y["States"].unique())
            Map_insur_District(Map_trans_tac_Y, states)

            quarters=st.slider("Select the Quarter_mt", Map_trans_tac_Y["Quarter"].min(),Map_trans_tac_Y["Quarter"].max(),Map_trans_tac_Y["Quarter"].min())
            Map_tran_tac_Y_Q= Transaction_amount_count_Y_Q(Map_trans_tac_Y,quarters)

            states=st.selectbox("Select The state_ma",Map_tran_tac_Y_Q["States"].unique())
            Map_insur_District(Map_tran_tac_Y_Q, states)

        elif method2=="Map User":
            years=st.slider("Select the Year_mu", Map_user["Year"].min(),Map_user["Year"].max(),Map_user["Year"].min())
            Map_user_Y= map_user_plot_1(Map_user,years)

            quarters=st.slider("Select the Quarter_mu", Map_user_Y["Quarter"].min(),Map_user_Y["Quarter"].max(),Map_user_Y["Quarter"].min())
            Map_user_Y_Q= map_user_plot_2(Map_user_Y,quarters)

            states=st.selectbox("Select The state_mu",Map_user_Y_Q["States"].unique())
            map_user_plot_3(Map_user_Y_Q, states)
    with tab3:
        method3=st.radio("Select The Method",["Top Insurance", "Top Transaction", "Top User"])
        if method3=="Top Insurance":
            years=st.slider("Select the Year_ti", Top_insurance["Year"].min(),Top_insurance["Year"].max(),Top_insurance["Year"].min())
            Top_insur_tac_Y=Transaction_amount_count_Y(Top_insurance,years)

            states=st.selectbox("Select The state_ti",Top_insur_tac_Y["States"].unique())
            Top_insur_plot_1(Top_insur_tac_Y, states)

            quarters=st.slider("Select the Quarter_ti", Top_insur_tac_Y["Quarter"].min(),Top_insur_tac_Y["Quarter"].max(),Top_insur_tac_Y["Quarter"].min())
            Top_insur_tac_Y_Q= Transaction_amount_count_Y_Q(Top_insur_tac_Y,quarters)

        elif method3=="Top Transaction":
            years=st.slider("Select the Year_tt", Top_transaction["Year"].min(),Top_transaction["Year"].max(),Top_transaction["Year"].min())
            Top_tran_tac_Y=Transaction_amount_count_Y(Top_transaction,years)

            states=st.selectbox("Select The state_tt",Top_tran_tac_Y["States"].unique())
            Top_insur_plot_1(Top_tran_tac_Y, states)

            quarters=st.slider("Select the Quarter_tt", Top_tran_tac_Y["Quarter"].min(),Top_tran_tac_Y["Quarter"].max(),Top_tran_tac_Y["Quarter"].min())
            Top_tran_tac_Y_Q= Transaction_amount_count_Y_Q(Top_tran_tac_Y,quarters)
        elif method3=="Top User":
            years=st.slider("Select the Year_tu", Top_user["Year"].min(),Top_user["Year"].max(),Top_user["Year"].min())
            Top_user_Y= Top_user_plot_1(Top_user,years)

            states=st.selectbox("Select The state_tu",Top_user_Y["States"].unique())
            Top_user_plot_2(Top_user_Y, states)
        
elif select=="TOP CHARTS":
    question= st.selectbox("Select the Question",['1.Transaction amount and count of Aggregated Insurance', '2.Transaction amount and count of Map Insurance', 
                                                    '3.Transaction amount and count of Top Insurance', 
                                                    '4.Transaction amount and count of Aggregated Transaction',
                                                    '5.Transaction amount and count of Map Transaction',
                                                    '6.Transaction amount and count of Top Transaction',
                                                    '7.Transaction  count of Aggregated User',
                                                    '8.Registered user of map user',
                                                    '9.App opens of Map User',
                                                    '10.Registered user of Top user'])
    
    if question =='1.Transaction amount and count of Aggregated Insurance':
        Top_chart_transaction_amount("aggregated_insurance")
        Top_chart_transaction_count("aggregated_insurance")

    elif question =='2.Transaction amount and count of Map Insurance':
        Top_chart_transaction_amount("map_insurance")
        Top_chart_transaction_count("map_insurance")

    elif question =='3.Transaction amount and count of Top Insurance':
        Top_chart_transaction_amount("top_insurance")
        Top_chart_transaction_count("top_insurance")

    elif question =='4.Transaction amount and count of Aggregated Transaction':
        Top_chart_transaction_amount("aggregated_transaction")
        Top_chart_transaction_count("aggregated_transaction")

    elif question =='5.Transaction amount and count of Map Transaction':
        Top_chart_transaction_amount("map_transaction")
        Top_chart_transaction_count("map_transaction")

    elif question =='6.Transaction amount and count of Top Transaction':
        Top_chart_transaction_amount("top_transaction")
        Top_chart_transaction_count("top_transaction")

    elif question =='7.Transaction  count of Aggregated User':
        Top_chart_transaction_count("aggregated_user")

    elif question =='8.Registered user of map user':
        states=st.selectbox("Select the states", Map_user["States"].unique())
        Top_chart_registered_user("map_user",states)

    elif question =='9.App opens of Map User':
        states=st.selectbox("Select the states", Map_user["States"].unique())
        Top_chart_appopens("map_user",states)

    elif question =='10.Registered user of Top user':
        Top_chart_registered_users("top_user")