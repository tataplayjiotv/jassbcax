import subprocess
import requests
import json
from datetime import datetime, timedelta
import os
import logging
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Set up logging with AM/PM format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y %I:%M:%S %p'
)

# Load the bot token from token.json (ensure this file is in the repository or handled securely)
def load_token():
    with open('token.json', 'r') as f:
        data = json.load(f)
    return data['token']

TELEGRAM_API_TOKEN = load_token()

def get_keys(start_time, end_time, channel_id=114, max_attempts=3):
    """Fetch decryption keys for the given time range"""
    for attempt in range(max_attempts):
        try:
            start_time = str(int(start_time))
            end_time = str(int(end_time))
            url = f"https://chkey.jasssaini.xyz/get_keys?id={channel_id}&begin={start_time}&end={end_time}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()["keys"]
        except Exception as e:
            logging.error(f"Failed to get keys (attempt {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
            else:
                return None

def detect_key_change_time(start_time, end_time, channel_id, interval=300):
    """Detect when keys change by sampling at intervals"""
    current_time = start_time
    previous_key = None
    key_changes = []
    
    while current_time < end_time:
        keys = get_keys(current_time, current_time + 60, channel_id)
        if not keys:
            logging.error(f"Failed to get keys at {datetime.fromtimestamp(current_time).strftime('%d-%m-%Y %I:%M %p')}")
            return key_changes
        
        current_key = keys[0]["key"] if keys else None
        if previous_key and current_key != previous_key:
            key_changes.append(current_time)
            logging.info(f"Key change detected at {datetime.fromtimestamp(current_time).strftime('%d-%m-%Y %I:%M %p')}")
        
        previous_key = current_key
        current_time += interval
    
    return key_changes

def generate_time_segments(start_dt, end_dt, channel_id):
    """Generate segments based on detected key changes"""
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    key_changes = detect_key_change_time(start_ts, end_ts, channel_id)
    segments = []
    current_start = start_ts
    
    for change_time in key_changes + [end_ts]:
        if current_start < change_time:
            segments.append((current_start, change_time))
            current_start = change_time
    
    if not segments or len(segments) == 1:
        current_time = start_dt.replace(second=0, microsecond=0)
        minutes_to_next = 15 - (current_time.minute % 15)
        if minutes_to_next < 15:
            current_time += timedelta(minutes=minutes_to_next)
        
        while current_time < end_dt:
            segment_start = int(current_time.timestamp())
            segment_end = int(min(current_time + timedelta(minutes=15), end_dt).timestamp())
            segments.append((segment_start, segment_end))
            current_time += timedelta(minutes=15)
    
    return segments

def download_segment(video_url, output_file, format_spec='bv', timeout=300):
    """Download MPD segment using yt-dlp with original command"""
    command = [
        'python', '-m', 'yt_dlp',
        '--geo-bypass-country', 'IN',
        '-k',
        '--allow-unplayable-formats',
        '--no-check-certificate',
        '--add-header', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.69.69.69 YGX/537.36',
        '--add-header', 'x-playback-session-id: b1222eddc62d6c9d',
        '--add-header', 'Referer: https://watch.tataplay.com/',
        '--add-header', 'Origin: https://watch.tataplay.com',
        '-f', format_spec,
        video_url,
        '-o', output_file
    ]
    
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=timeout)
        logging.info(f"Successfully downloaded {output_file}")
        return True
    except subprocess.TimeoutExpired as e:
        logging.error(f"Download timeout for {output_file}: {e.stdout}\n{e.stderr}")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Download failed for {output_file}: {e.stdout}\n{e.stderr}")
        return False

def decrypt_and_merge(video_file, audio_file, output_file, key):
    """Decrypt and merge video and audio with stream synchronization"""
    command = [
        'ffmpeg',
        '-decryption_key', key,
        '-i', video_file,
        '-decryption_key', key,
        '-i', audio_file,
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-map', '0:v:0',
        '-map', '1:a:0',
        '-vsync', '2',
        '-async', '1',
        '-shortest',
        '-fflags', '+genpts',
        '-y',
        output_file
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=300)
        logging.info(f"Successfully merged {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Merge failed for {output_file} (Chat ID: {update.message.chat_id}): {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Merge failed: {e}")
        return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🌟 <b>Welcome to the World's Best Tata Play MPD Downloader Bot!</b> 🌟\n\n"
        "✨ Send the date and time range in this format:\n"
        "<code>DD-MM-YYYY HH:MM AM/PM - DD-MM-YYYY HH:MM AM/PM</code>\n"
        "📅 Example: <code>04-04-2025 11:00 AM - 04-04-2025 12:30 PM</code>\n\n"
        "🎥 Supports up to 6 hours of video with stunning animations!",
        parse_mode='HTML'
    )

async def upload_progress(context, chat_id, message_id, file_size, uploaded_size):
    """Update upload progress in Telegram"""
    percent = min((uploaded_size / file_size) * 100, 100) if file_size > 0 else 0
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=(
            "🚀 <b>Uploading Video</b> 🚀\n"
            f"📏 <b>Size:</b> {file_size / (1024 * 1024):.2f} MB\n"
            f"📈 <b>Progress:</b> {percent:.1f}%\n"
            "⏳ <i>Uploading, please wait...</i>"
        ),
        parse_mode='HTML'
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text
    chat_id = update.message.chat_id
    channel_id = 239
    base_url = "https://jasssaini.xyz/tplay/manifet.php"
    output_dir = f"downloads_{chat_id}"
    os.makedirs(output_dir, exist_ok=True)

    date_format = "%d-%m-%Y %I:%M %p"
    try:
        start_str, end_str = user_input.split(" - ")
        start_dt = datetime.strptime(start_str.strip(), date_format)
        end_dt = datetime.strptime(end_str.strip(), date_format)
    except ValueError:
        await update.message.reply_text(
            "❌ <b>Invalid format!</b> Please use:\n"
            "<code>DD-MM-YYYY HH:MM AM/PM - DD-MM-YYYY HH:MM AM/PM</code>\n"
            "📅 Example: <code>04-04-2025 11:00 AM - 04-04-2025 12:30 PM</code>",
            parse_mode='HTML'
        )
        return

    duration_hours = (end_dt - start_dt).total_seconds() / 3600
    if start_dt >= end_dt or duration_hours > 6:
        await update.message.reply_text("❌ <b>Start time must be before end time and duration must not exceed 6 hours!</b>", parse_mode='HTML')
        return

    processing_msg = await update.message.reply_text(
        "📡 <b>Processing MPD Download</b> 📡\n"
        "⏳ <i>Initializing...</i>\n"
        f"⏰ <b>From:</b> {start_dt.strftime('%d-%m-%Y %I:%M %p')}\n"
        f"⏰ <b>To:</b> {end_dt.strftime('%d-%m-%Y %I:%M %p')}\n"
        "✨ <b>Please wait...</b>",
        parse_mode='HTML'
    )

    segments = generate_time_segments(start_dt, end_dt, channel_id)
    video_files = []
    audio_files = []
    merged_files = []

    for i, (segment_start, segment_end) in enumerate(segments):
        start_time_str = datetime.fromtimestamp(segment_start).strftime('%d-%m-%Y_%I-%M_%p')
        end_time_str = datetime.fromtimestamp(segment_end).strftime('%d-%m-%Y_%I-%M_%p')
        logging.info(f"Processing segment {i}: {start_time_str} to {end_time_str}")

        await context.bot.edit_message_text(
            chat_id=processing_msg.chat_id,
            message_id=processing_msg.message_id,
            text=(
                "📡 <b>Processing MPD Download</b> 📡\n"
                f"⏳ <i>Segment {i + 1}/{len(segments)}...</i>\n"
                f"⏰ <b>From:</b> {start_dt.strftime('%d-%m-%Y %I:%M %p')}\n"
                f"⏰ <b>To:</b> {end_dt.strftime('%d-%m-%Y %I:%M %p')}\n"
                "✨ <b>Downloading...</b>"
            ),
            parse_mode='HTML'
        )

        keys = get_keys(segment_start, segment_end, channel_id)
        if not keys:
            await update.message.reply_text(f"⚠️ Failed to fetch keys for segment {i} (Chat ID: {chat_id}).")
            return

        key = keys[0]["key"]
        video_url = f"{base_url}?id={channel_id}&begin={segment_start}&end={segment_end}"

        video_output = os.path.join(output_dir, f"segment_{i}_video_{start_time_str}.mp4")
        audio_output = os.path.join(output_dir, f"segment_{i}_audio_{start_time_str}.m4a")
        merged_output = os.path.join(output_dir, f"segment_{i}_merged_{start_time_str}.mkv")

        if download_segment(video_url, video_output, 'bv'):
            video_files.append(video_output)
            if download_segment(video_url, audio_output, 'ba'):
                audio_files.append(audio_output)
                if decrypt_and_merge(video_output, audio_output, merged_output, key):
                    merged_files.append(merged_output)
                else:
                    await update.message.reply_text(f"⚠️ Failed to merge segment {i} (Chat ID: {chat_id}).")
                    return
            else:
                await update.message.reply_text(f"⚠️ Failed to download audio for segment {i} (Chat ID: {chat_id}).")
                return
        else:
            await update.message.reply_text(f"⚠️ Failed to download video for segment {i} (Chat ID: {chat_id}).")
            return

    final_output = f"final_output_{channel_id}_{start_dt.strftime('%d-%m-%Y_%I-%M_%p')}_to_{end_dt.strftime('%d-%m-%Y_%I-%M_%p')}.mkv"
    final_path = os.path.abspath(os.path.join(output_dir, final_output))
    os.chdir(output_dir)
    with open('file_list.txt', 'w') as f:
        for merged_file in merged_files:
            f.write(f"file '{os.path.basename(merged_file)}'\n")

    final_command = [
        'ffmpeg', '-f', 'concat', '-safe', '0', '-i', 'file_list.txt',
        '-c:v', 'copy', '-c:a', 'copy', '-fflags', '+genpts', '-y', final_output
    ]
    try:
        subprocess.run(final_command, check=True, capture_output=True, text=True, timeout=1200)
    except subprocess.CalledProcessError as e:
        logging.error(f"Concatenation failed: {e.stderr}")
        await update.message.reply_text(f"⚠️ Failed to concatenate segments (Chat ID: {chat_id}).")
        os.chdir(os.path.dirname(os.getcwd()))
        return

    os.chdir(os.path.dirname(os.getcwd()))
    file_size = os.path.getsize(final_path)
    if file_size < 1024 * 1024 or file_size > 2048 * 1024 * 1024:
        await update.message.reply_text(f"⚠️ File size invalid: {file_size / (1024 * 1024):.2f} MB (Must be 1 MB to 2 GB)")
        return

    uploading_msg = await context.bot.edit_message_text(
        chat_id=processing_msg.chat_id,
        message_id=processing_msg.message_id,
        text=(
            "🚀 <b>Uploading Video</b> 🚀\n"
            f"📏 <b>Size:</b> {file_size / (1024 * 1024):.2f} MB\n"
            "📈 <b>Progress:</b> 0.0%\n"
            "⏳ <i>Uploading, please wait...</i>"
        ),
        parse_mode='HTML'
    )

    duration = (end_dt - start_dt).total_seconds() / 60
    description = (
        "🎬 <b>Channel:</b> Tata Play Channel\n"
        f"🆔 <b>Channel ID:</b> {channel_id}\n"
        f"📅 <b>Date:</b> {start_dt.strftime('%d-%m-%Y')}\n"
        f"⏰ <b>Time:</b> {start_dt.strftime('%I:%M %p')} - {end_dt.strftime('%I:%M %p')}\n"
        f"⏱️ <b>Duration:</b> {duration:.1f} Minutes\n"
        "📼 <b>Quality:</b> Original (Decrypted MPD)\n"
        f"📏 <b>Size:</b> {file_size / (1024 * 1024):.2f} MB\n"
        "🌐 <b>Source:</b> Tata Play\n"
        "👤 <b>Encoded by:</b> Jass\n"
        "✨ <b>Powered by:</b> World's Best MPD Downloader"
    )

    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            async def upload_with_progress():
                with open(final_path, 'rb') as video:
                    uploaded = 0
                    chunk_size = 50 * 1024 * 1024
                    while True:
                        chunk = video.read(chunk_size)
                        if not chunk:
                            break
                        uploaded += len(chunk)
                        if uploaded % (200 * 1024 * 1024) == 0 or uploaded == file_size:
                            await upload_progress(context, chat_id, uploading_msg.message_id, file_size, uploaded)
                    video.seek(0)
                    await update.message.reply_video(
                        video=video,
                        caption=description,
                        parse_mode='HTML',
                        supports_streaming=True
                    )

            await upload_with_progress()
            await context.bot.delete_message(chat_id=chat_id, message_id=uploading_msg.message_id)
            break

        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Upload failed (Attempt {attempt + 1}/{max_retries}): {str(e)}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                await update.message.reply_text(f"⚠️ Upload failed after {max_retries} attempts: {str(e)} (Chat ID: {chat_id})")
                await context.bot.delete_message(chat_id=chat_id, message_id=uploading_msg.message_id)
                return

    for file in video_files + audio_files + merged_files + [final_path, os.path.join(output_dir, 'file_list.txt')]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(output_dir):
        os.rmdir(output_dir)
    logging.info("Cleanup completed successfully")

def main():
    application = Application.builder().token(TELEGRAM_API_TOKEN).read_timeout(21600).write_timeout(21600).build()
    print("🚀 Bot started successfully!")
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    application.run_polling()

if __name__ == "__main__":
    main()