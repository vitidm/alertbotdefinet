import time
import json
import requests
import logging
import random
from telegram.ext import Updater, CommandHandler
from datetime import datetime, timedelta
import configparser
import mysql.connector
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sync import TelegramClient
from telethon import functions
from telegram.ext import Updater, CommandHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# Buttons creation
buttons = [[InlineKeyboardButton('Visit DefiNet🤏', url='https://definet.fly.dev/')]]
keyboard = InlineKeyboardMarkup(buttons)

API_URL_cryptoCompare = 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms=ETH&tsyms=USD'

def sqlConnectorExtractAllDataTokens(filtered_date):
    cnx = mysql.connector.connect(
        host='sql8.freesqldatabase.com',
        user='sql8593502',
        password='tuz9qrT3jT',
        database='sql8593502',
        port=3306
    )
    cursor = cnx.cursor(dictionary=True)
    # WHERE time like '%{filtered_date}%'
    query = f"""SELECT * FROM all_data_tokens"""
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    cnx.close()
    
    return result

def reduce_number(num):
    if num >= 1000000:
        return str(round(num / 1000000, 1)) + "M"
    elif num >= 1000:
        return str(round(num / 1000)) + "K"
    else:
        return str(num)
    
def getEthPrice():
    response = requests.get(API_URL_cryptoCompare)
    data = response.json()
    price = data['RAW']['ETH']['USD']['PRICE']
    variation = data['RAW']['ETH']['USD']['CHANGEPCT24HOUR']

    return price, variation

def get_gwei():
    url = "https://mainnet.infura.io/v3/a69b219fbd54407faa5af30d764526ef"
    headers = {"Content-Type": "application/json"}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_gasPrice",
        "params": []
    }
    response = requests.post(url, headers=headers, json=payload)
    data = response.json()
    gas_price = int(data["result"], 16)
    gwei = gas_price / 1000000000
    return gwei

def formatData():
    current_date = datetime.now().strftime('%d/%m/%Y')
    dataByToken = sqlConnectorExtractAllDataTokens(current_date)
    hour1_by_date = {}

    # Modificar hour1_by_date para tener la suma de valores por fecha
    for data in dataByToken:
        for key, value in data.items():
            if key == 'time':
                date_string = value.split(' ')[0]
                date_obj = datetime.strptime(date_string, '%d/%m/%Y')
                hour_string = value.split(' ')[1].split(':')[0]
                date_hour_string = date_obj.strftime('%d/%m/%Y ') + hour_string    
            elif key == 'hour1':
                hour1_value = value
                    
                token = data['token_address']
                if date_hour_string not in hour1_by_date:
                    hour1_by_date[date_hour_string] = {}
                if token not in hour1_by_date[date_hour_string]:
                    hour1_by_date[date_hour_string][token] = hour1_value
                else:
                    hour1_by_date[date_hour_string][token] += hour1_value
    
    # Calcular la suma total de los valores para cada hora en los días que cumplen la condición
    total_hour1_by_date_hour = {}
    for date_hour_string, hour1_value in hour1_by_date.items():
        total_hour1 = sum(hour1_value.values())
        if total_hour1 < 1000000:
            hour_string = date_hour_string[-2:]
            date_string = date_hour_string[:-3]
            if date_string not in total_hour1_by_date_hour:
                total_hour1_by_date_hour[date_string] = {}
            if hour_string not in total_hour1_by_date_hour[date_string]:
                total_hour1_by_date_hour[date_string][hour_string] = []
            total_hour1_by_date_hour[date_string][hour_string].append(total_hour1)
    
    total_hour1_by_date = {}

    sum_by_date = {}
    for date_string, hours_by_hour in total_hour1_by_date_hour.items():
        sum_by_date[date_string] = sum(sum(hours) for hours in hours_by_hour.values())

    current_price_eth, variation_price_eth = getEthPrice()
    current_gwei = "{:,.1f}".format(get_gwei())
    
    price_eth_arrow = ""
    # Variation ETH Price Arrow
    if variation_price_eth > 0:
        price_eth_arrow = "📈"
    else:
        price_eth_arrow = "📉"

    # Final Transformations if current date
    if current_date in sum_by_date:
        current_value = sum_by_date[current_date]
        current_value_eth_format = '{:,.0f}'.format(current_value / current_price_eth)
        formatted_value = "{:,}".format(current_value)
        variation_price_eth = '{:,.2f}'.format(variation_price_eth)
        
        print(f"ETH Price: {current_price_eth} [{variation_price_eth}% {price_eth_arrow}]")
        print(f"Current volume for today ({current_date}): ${formatted_value} ({current_value_eth_format} ETH)")
        split_data = {}
        for date, value in sum_by_date.items():
            split_data[date] = []
            part_size = value / 5
            cumulative_sum = 0
            for i in range(1, 6):
                cumulative_sum += i * part_size
                split_data[date].append(cumulative_sum)

        average_data = [0, 0, 0, 0, 0]

        for date in split_data:
            for i in range(5):
                average_data[i] += split_data[date][i]

        for i in range(5):
            average_data[i] /= len(split_data)

        average_data_sorted = sorted(average_data, reverse=False)
        average_data_sorted.insert(0, 0)

        if average_data_sorted[0] <= current_value < average_data_sorted[1]:
            category_text = 'Bad'
            range_text = f"(${reduce_number(average_data_sorted[0])} - ${reduce_number(average_data_sorted[1])})"
            
        elif average_data_sorted[1] <= current_value < average_data_sorted[2]:
            category_text = 'Low'
            range_text = f"(${reduce_number(average_data_sorted[1])} - ${reduce_number(average_data_sorted[2])})"
            
        elif average_data_sorted[2] <= current_value < average_data_sorted[3]:
            category_text = 'Mid'
            range_text = f"(${reduce_number(average_data_sorted[2])} - ${reduce_number(average_data_sorted[3])})"
            
        elif average_data_sorted[3] <= current_value < average_data_sorted[4]:
            category_text = 'Good'
            range_text = f"(${reduce_number(average_data_sorted[3])} - ${reduce_number(average_data_sorted[4])})"
            
        else:
            category_text = 'High'
            range_text = f"(${reduce_number(average_data_sorted[4])} - ${reduce_number(average_data_sorted[5])})"
            
    else:
        print("No value for today in the dictionary.")

    return current_price_eth, current_gwei, variation_price_eth, price_eth_arrow, current_date, formatted_value, current_value_eth_format, category_text, range_text

def volume(update, context):
    current_price_eth, current_gwei, variation_price_eth, price_eth_arrow, current_date, formatted_value, current_value_eth_format, category_text, range_text = formatData()
    context.bot.send_message(chat_id=update.message.chat_id, text=f"<b>💹Market Status: </b>{category_text}\n<b>📈{category_text}-Range:</b>{range_text}<b>\n<b>💰 24h DEFI Vol:</b> ${formatted_value}\n<b>📊 24h DEFI Vol(ETH):</b>{current_value_eth_format} ETH\n\n⚡️ETH Price:</b> ${current_price_eth}\n<b>{price_eth_arrow}ETH Variation:</b>{variation_price_eth}% \n<b>⛽️ Gas Price:</b>{current_gwei}\n\n <u><a href='https://t.me/definetofficialportal'>TG</a></u> <u><a href='https://www.dextools.io/app/es/ether/pair-explorer/0x5935c4651db7be150416d600fb1cc21753785678'>Chart</a></u> <u><a href='https://twitter.com/DefiNetdApp'>TW</a></u>", reply_markup=keyboard,parse_mode='HTML', disable_web_page_preview=True)
    
    # Info Group and People
    chat_name = update.message.chat.title
    chat_id = update.message.chat_id
    user_id = update.message.from_user.id
    username = update.message.from_user.username
    name = update.message.from_user.full_name
    print(f"Group: {chat_name} | Group Id: {chat_id} ||| User Id: {user_id} | Username: {username} | User Name: {name}")


# Función que se ejecuta cuando se recibe el comando /stop
def stop(update, context):
    updater.stop()
    print("You can press Ctrl + C")
    
# crea un Updater y un dispatcher
updater = Updater(token='6247270046:AAF1-w6C6TUekXqIQKY4qzhlkcLoj679W5U', use_context=True)
dispatcher = updater.dispatcher

# agrega un handler para el comando /volume
# Se agrega el manejador de comando "/volume"
updater.dispatcher.add_handler(CommandHandler('volume', volume))

# Se agrega el manejador de comando "/stopbot"
updater.dispatcher.add_handler(CommandHandler('stopbot', stop))

# inicia el bot
updater.start_polling()
# Mantener al bot en ejecución hasta que se reciba el comando "/stop"
updater.idle()
