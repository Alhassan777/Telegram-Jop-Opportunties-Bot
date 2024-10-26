import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import markdown
from telegram import Update
from telegram.constants import ParseMode

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
import os
import sqlite3

from dotenv import load_dotenv
load_dotenv()

# URL of the raw README.md file containing the internships
GITHUB_RAW_URL = "https://raw.githubusercontent.com/SimplifyJobs/Summer2025-Internships/master/README.md"

# Your Telegram bot token
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')

# Database file
DB_FILE = 'users.db'

# Constants for conversation states
SET_TIME, SET_FREQUENCY = range(2)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            update_time TEXT DEFAULT '09:00',  -- Default update time is 09:00 AM UTC
            frequency INTEGER DEFAULT 24      -- Default frequency is every 24 hours
        )
    ''')
    conn.commit()
    conn.close()

def migrate_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # Get current columns
    cursor.execute("PRAGMA table_info(users)")
    columns = [info[1] for info in cursor.fetchall()]
    # Add 'update_time' column if it doesn't exist
    if 'update_time' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN update_time TEXT DEFAULT '09:00'")
    # Add 'frequency' column if it doesn't exist
    if 'frequency' not in columns:
        cursor.execute("ALTER TABLE users ADD COLUMN frequency INTEGER DEFAULT 24")
    conn.commit()
    conn.close()

def add_user(chat_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO users (chat_id)
        VALUES (?)
    ''', (chat_id,))
    conn.commit()
    conn.close()

def remove_user(chat_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM users WHERE chat_id = ?', (chat_id,))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT chat_id, update_time, frequency FROM users')
    users = cursor.fetchall()
    conn.close()
    return users

def get_user_preferences(chat_id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('SELECT update_time, frequency FROM users WHERE chat_id = ?', (chat_id,))
    result = cursor.fetchone()
    conn.close()
    return result if result else ('09:00', 24)

def update_user_time(chat_id, update_time):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET update_time = ? WHERE chat_id = ?', (update_time, chat_id))
    conn.commit()
    conn.close()

def update_user_frequency(chat_id, frequency):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET frequency = ? WHERE chat_id = ?', (frequency, chat_id))
    conn.commit()
    conn.close()

async def get_internships():
    try:
        response = requests.get(GITHUB_RAW_URL)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return []
    except Exception as err:
        print(f"An error occurred: {err}")
        return []

    # Convert markdown to HTML
    html = markdown.markdown(response.text, extensions=['tables'])

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find all tables in the HTML
    tables = soup.find_all('table')
    if not tables:
        print("Error: Could not find any table element on the page.")
        return []

    # Assuming the internships are in the first table
    table = tables[0]

    # Extract table rows
    tbody = table.find('tbody')
    rows = tbody.find_all('tr') if tbody else table.find_all('tr')

    results = []

    # Get table headers to map data correctly
    headers = [th.get_text(strip=True) for th in table.find_all('th')]

    # Iterate over rows and extract data based on headers
    for row in rows:
        data_cells = row.find_all('td')
        if not data_cells or len(data_cells) != len(headers):
            # Skip rows that don't match the header length
            continue

        internship_entry = {}
        for header, cell in zip(headers, data_cells):
            text = cell.get_text(strip=True)
            if header == 'Company':
                # Improved extraction logic for company name and link
                company_name_tag = cell.find('a') or cell.find('strong') or cell
                company_name = company_name_tag.get_text(strip=True)
                company_link_tag = company_name_tag if company_name_tag.name == 'a' else company_name_tag.find('a')
                company_link = company_link_tag.get('href') if company_link_tag else None
                # If company link is not found at this level, check in cell
                if not company_link:
                    company_link_tag = cell.find('a')
                    company_link = company_link_tag.get('href') if company_link_tag else None
                internship_entry['Company'] = company_name
                internship_entry['Link'] = company_link
            elif header == 'Role':
                internship_entry['Role'] = text
            elif header == 'Location':
                location = text.replace('\n', ', ')
                internship_entry['Location'] = location
            elif header == 'Application/Link':
                application_tag = cell.find('a')
                if application_tag:
                    application_link = application_tag.get('href')
                else:
                    application_link = None
                internship_entry['Application/Link'] = application_link
            elif header == 'Date Posted':
                try:
                    date_posted = parse_date(text)
                except ValueError:
                    date_posted = None
                internship_entry['Date Posted'] = date_posted
            else:
                internship_entry[header] = text

        # Skip entries with missing company names
        if internship_entry.get('Company', '').strip() == '':
            continue

        # Add the complete entry to the results
        results.append(internship_entry)

    return results

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    add_user(chat_id)
    await update.message.reply_text(
        "Welcome to the Internship Bot! You have been subscribed to updates.\n"
        "Use /settime to set your preferred update time.\n"
        "Use /setfrequency to set how often you want to receive updates.\n"
        "Use /updates [number] to get the latest internships immediately."
    )
    # Schedule the job for the new user
    schedule_user_job(context.application.job_queue, chat_id)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start - Subscribe to updates\n"
        "/stop - Unsubscribe from updates\n"
        "/settime - Set your preferred update time\n"
        "/setfrequency - Set how often you want to receive updates\n"
        "/updates [number] - Get the latest internships immediately (optional number of internships)\n"
        "/help - Show this help message"
    )

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    remove_user(chat_id)
    await update.message.reply_text("You have been unsubscribed from updates.")
    # Remove scheduled jobs for this user
    jobs = context.application.job_queue.get_jobs_by_name(str(chat_id))
    for job in jobs:
        job.schedule_removal()

async def updates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    num_internships = 5  # Default value
    if args:
        try:
            num_internships = int(args[0])
            if num_internships <= 0:
                await update.message.reply_text("Please enter a positive number.")
                return
        except ValueError:
            await update.message.reply_text("Please provide a valid number of internships.")
            return

    max_internships = 50  # Set a maximum limit to prevent overloading
    if num_internships > max_internships:
        num_internships = max_internships
        await update.message.reply_text(f"Limiting to the first {max_internships} internships.")

    internships = await get_internships()
    if not internships:
        await update.message.reply_text("No internships found.")
        return

    latest_internships = internships[:num_internships]

    messages = []
    current_message = ""
    for internship in latest_internships:
        date_posted = internship.get('Date Posted')
        date_posted_str = date_posted.strftime('%b %d, %Y') if date_posted else "N/A"
        internship_message = (
            f"*Company*: {internship.get('Company', 'N/A')}\n"
            f"*Role*: {internship.get('Role', 'N/A')}\n"
            f"*Location*: {internship.get('Location', 'N/A')}\n"
            f"[Link]({internship.get('Link', 'N/A')})\n"
            f"[Application]({internship.get('Application/Link', 'N/A')})\n"
            f"*Date Posted*: {date_posted_str}\n\n"
        )
        if len(current_message) + len(internship_message) > 4000:
            messages.append(current_message)
            current_message = internship_message
        else:
            current_message += internship_message

    if current_message:
        messages.append(current_message)

    for message in messages:
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def set_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Please enter the time you want to receive updates each day (in HH:MM format, UTC):"
    )
    return SET_TIME

async def receive_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_input = update.message.text
    try:
        # Validate time format
        datetime.strptime(user_input, '%H:%M')
        update_user_time(chat_id, user_input)
        await update.message.reply_text(f"Your update time has been set to {user_input} UTC.")
        # Reschedule the job for this user
        schedule_user_job(context.application.job_queue, chat_id)
    except ValueError:
        await update.message.reply_text("Invalid time format. Please enter in HH:MM format (e.g., 21:00).")
        return SET_TIME
    return ConversationHandler.END

async def set_frequency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Please enter how often you want to receive updates (in hours, e.g., 8, 12, 24):"
    )
    return SET_FREQUENCY

async def receive_frequency(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_input = update.message.text
    try:
        frequency = int(user_input)
        if frequency <= 0:
            raise ValueError
        update_user_frequency(chat_id, frequency)
        await update.message.reply_text(f"Your update frequency has been set to every {frequency} hours.")
        # Reschedule the job for this user
        schedule_user_job(context.application.job_queue, chat_id)
    except ValueError:
        await update.message.reply_text("Invalid input. Please enter a positive integer (e.g., 8, 12, 24).")
        return SET_FREQUENCY
    return ConversationHandler.END

def schedule_user_job(job_queue, chat_id):
    # Remove existing jobs for this user
    jobs = job_queue.get_jobs_by_name(str(chat_id))
    for job in jobs:
        job.schedule_removal()

    update_time_str, frequency = get_user_preferences(chat_id)
    hour, minute = map(int, update_time_str.split(':'))
    frequency_timedelta = timedelta(hours=frequency)

    # Schedule a new job for this user
    next_run_time = datetime.utcnow().replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run_time < datetime.utcnow():
        next_run_time += timedelta(days=1)

    job_queue.run_repeating(
        send_scheduled_update,
        interval=frequency_timedelta,
        first=next_run_time,
        data=chat_id,
        name=str(chat_id)
    )

async def send_scheduled_update(context: ContextTypes.DEFAULT_TYPE):
    chat_id = context.job.data
    internships = await get_internships()
    if not internships:
        return

    # Load previously sent internships to avoid duplicates
    sent_internships_file = f'sent_internships_{chat_id}.txt'
    if os.path.exists(sent_internships_file):
        with open(sent_internships_file, 'r') as f:
            sent_internships = set(f.read().splitlines())
    else:
        sent_internships = set()

    new_internships = []
    for internship in internships:
        identifier = internship.get('Company', 'N/A') + internship.get('Role', 'N/A')
        if identifier not in sent_internships:
            new_internships.append(internship)
            sent_internships.add(identifier)

    if not new_internships:
        return

    # Save updated sent internships
    with open(sent_internships_file, 'w') as f:
        for identifier in sent_internships:
            f.write(f"{identifier}\n")

    messages = []
    current_message = ""
    for internship in new_internships:
        date_posted = internship.get('Date Posted')
        date_posted_str = date_posted.strftime('%b %d, %Y') if date_posted else "N/A"
        message_parts = [
            f"*Company*: {internship.get('Company', 'N/A')}",
            f"*Role*: {internship.get('Role', 'N/A')}",
            f"*Location*: {internship.get('Location', 'N/A')}",
        ]

        # Include the link only if it's available
        link = internship.get('Link')
        if link:
            message_parts.append(f"[Link]({link})")

        # Remove the 'Application' line as per your request

        message_parts.append(f"*Date Posted*: {date_posted_str}")

        # Combine all parts into the final message
        internship_message = '\n'.join(message_parts) + '\n\n'
        if len(current_message) + len(internship_message) > 4000:
            messages.append(current_message)
            current_message = internship_message
        else:
            current_message += internship_message

    if current_message:
        messages.append(current_message)

    for message in messages:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            print(f"Failed to send message to {chat_id}: {e}")

def main():
    # Initialize the database
    init_db()
    # Migrate the database schema
    migrate_db()

    # Create the Application and pass the bot's token
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Ensure that JobQueue is properly initialized
    job_queue = application.job_queue

    if job_queue is None:
        print("JobQueue is not initialized correctly. Ensure the proper version of python-telegram-bot is installed.")
        return

    # Command handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('stop', stop))
    application.add_handler(CommandHandler('updates', updates_command))

    # Conversation handlers for setting time and frequency
    time_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('settime', set_time)],
        states={
            SET_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_time)],
        },
        fallbacks=[],
    )
    frequency_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('setfrequency', set_frequency)],
        states={
            SET_FREQUENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_frequency)],
        },
        fallbacks=[],
    )

    application.add_handler(time_conv_handler)
    application.add_handler(frequency_conv_handler)

    # Schedule the jobs for existing users
    for chat_id, update_time_str, frequency in get_all_users():
        schedule_user_job(job_queue, chat_id)

    # Start the bot
    application.run_polling()

if __name__ == '__main__':
    main()
