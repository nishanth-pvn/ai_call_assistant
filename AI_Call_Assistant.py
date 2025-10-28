import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import os
from PIL import Image
import requests
from io import BytesIO
import hashlib
import time
import re
import whisper
from audio_recorder_streamlit import audio_recorder
import tempfile
import json
import pyttsx3
import threading
import soundfile as sf
import numpy as np

                                         
# Configure page
st.set_page_config(
    page_title="AI Call Assistant — Next Visit Prep",
    page_icon="📞",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS (reuse look-and-feel)
st.markdown("""
<style>
    .main > div { padding-top: 1rem; padding-bottom: 1rem; }
    .stSelectbox > div > div > div { font-size: 0.85rem; }
    .stRadio > div { font-size: 0.85rem; }
    .insight-container {
        background-color: #ffffff; padding: 15px; border-radius: 8px;
        border-left: 4px solid #1f77b4; margin: 10px 0; font-size: 0.9rem; line-height: 1.4;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
    }
    .forecast-container {
        background-color: #f8f9fa; padding: 15px; border-radius: 8px;
        border-left: 4px solid #28a745; margin: 10px 0; font-size: 0.9rem; line-height: 1.4;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
    }
    .insight-container h4, .forecast-container h4 {
        font-weight: 600; margin-bottom: 8px; color: #333;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif;
    }
    .insight-container p, .forecast-container p { font-weight: 400; margin-bottom: 8px; color: #555; font-size: 0.9rem; }
    .insight-container strong, .forecast-container strong { font-weight: 600; font-size: 0.9rem; }
    .header-container { display: flex; align-items: center; justify-content: center; margin-bottom: 1rem; gap: 15px; }
    .header-logo { display: flex; align-items: center; }
    .centered-title { text-align: center; margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif; }
    .stDataFrame > div { width: 100% !important; }
    .stDataFrame [data-testid="stDataFrameResizeHandle"] { display: block !important; }
    .stDataFrame table { width: 100% !important; table-layout: auto !important; }
    .stDataFrame th { min-width: 80px !important; padding: 8px 12px !important; font-weight: 600 !important; }
    .stDataFrame td { min-width: 80px !important; padding: 6px 12px !important; text-align: left !important; }
    .stMarkdown, .stText, p, div { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica', 'Arial', sans-serif; }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_logo(logo_path, width=60):
    """Load and resize a logo image."""
    try:
        if os.path.exists(logo_path):
            image = Image.open(logo_path)
            aspect_ratio = image.height / image.width
            height = int(width * aspect_ratio)
            image = image.resize((width, height), Image.Resampling.LANCZOS)
            return image
        return None
    except Exception as e:
        st.error(f"Error loading logo: {str(e)}")
        return None

@st.cache_data
def load_data(file_bytes: bytes | None = None, filename: str | None = None):
    """Load call history from multiple Excel files and combine them."""
    try:
        # List of Excel files to load
        excel_files = [
            "Call Basic Information with Call Notes.xlsx",
            "Call Attendees.xlsx", 
            "Call CLM.xlsx",
            "Call Key Messages.xlsx"
        ]
        
        all_dataframes = []
        
        if file_bytes is not None:
            # Handle uploaded file
            df = pd.read_excel(BytesIO(file_bytes))
            all_dataframes.append(df)
        else:
            # Load all available Excel files
            for excel_file in excel_files:
                if os.path.exists(excel_file):
                    try:
                        df = pd.read_excel(excel_file)
                        # Add source file identifier
                        df['_source_file'] = excel_file.replace('.xlsx', '')
                        all_dataframes.append(df)
                    except Exception as e:
                        st.sidebar.warning(f"⚠️ Could not load {excel_file}: {str(e)}")
        
        if not all_dataframes:
            st.error("No Excel files found to load")
            return None
        
        # Combine all dataframes
        if len(all_dataframes) == 1:
            df = all_dataframes[0]
        else:
            # Standardize column names for merging based on actual column names
            column_mappings = {
                # From Call Basic Information with Call Notes - map to call_name
                'Interaction: Call Name': 'call_name',
                'Account: Account Record Type': 'account_record_type',
                'Account: 18 Digit Account ID': 'account_id',
                'Account: External ID': 'external_id',
                'Territory': 'territory',
                'Account: Country Code': 'country_code',
                
                # From Call CLM and Call Key Messages - map ACTIV_NAME to call_name  
                'ACTIV_NAME': 'call_name',
                'KM_MKT_PROD_NAME': 'product_name',
                'KEYMSG_NAME': 'key_message',
                'KEYMSG_DESCR': 'key_message_description',
                'KEYMSG_MEDIA_FILE_NAME': 'media_file',
                
                # From Call Attendees - these will be concatenated since they don't have call_name
                'BUS_PARTY_IND_TYPE': 'attendee_type',
                'BUS_TITLE': 'attendee_title',
                'SPECIALITY_1': 'specialty_1',
                'SPECIALITY_2': 'specialty_2',
                'TOP_PRNT_PARTY': 'parent_party'
            }
            
            # Apply column mappings to each dataframe
            standardized_dataframes = []
            for df_temp in all_dataframes:
                df_copy = df_temp.copy()
                for old_name, new_name in column_mappings.items():
                    if old_name in df_copy.columns:
                        df_copy[new_name] = df_copy[old_name]
                        # Keep the original column name as well for reference
                standardized_dataframes.append(df_copy)
            
            # Find common columns after standardization
            common_cols = set(standardized_dataframes[0].columns)
            for df_temp in standardized_dataframes[1:]:
                common_cols = common_cols.intersection(set(df_temp.columns))
            
            # Remove the source file column from common columns
            common_cols.discard('_source_file')
            common_cols = list(common_cols)
            
            # Check if we have common columns to merge on (including standardized ones)
            if len(common_cols) == 0:
                # Concatenate all dataframes
                df = pd.concat(all_dataframes, ignore_index=True, sort=False)
            else:
                
                # Merge dataframes on common columns, handling multiple records per call
                df = standardized_dataframes[0]
                for i, df_temp in enumerate(standardized_dataframes[1:]):
                    try:
                        # For files with multiple records per call (like CLM, Key Messages, Attendees), 
                        # we need to aggregate them before merging
                        source_file = all_dataframes[i+1]['_source_file'].iloc[0] if '_source_file' in all_dataframes[i+1].columns else 'unknown'
                        
                        if 'CLM' in source_file or 'Key Messages' in source_file or 'Attendees' in source_file:
                            # Aggregate multiple records per call_name
                            agg_dict = {}
                            for col in df_temp.columns:
                                if col not in common_cols and col != '_source_file':
                                    if df_temp[col].dtype == 'object':  # Text columns
                                        # Combine text values with semicolon separator, removing duplicates
                                        agg_dict[col] = lambda x: '; '.join(x.dropna().astype(str).unique())
                                    else:  # Numeric columns
                                        # Take the first non-null value
                                        agg_dict[col] = 'first'
                            
                            if agg_dict:  # Only aggregate if there are columns to aggregate
                                df_temp_agg = df_temp.groupby('call_name', as_index=False).agg(agg_dict)
                                # Merge with aggregated data
                                df = pd.merge(df, df_temp_agg, on=common_cols, how='left', suffixes=('', '_dup'))
                            else:
                                # If no columns to aggregate, just get unique call_names
                                df_temp_agg = df_temp[['call_name']].drop_duplicates()
                                df = pd.merge(df, df_temp_agg, on=common_cols, how='left', suffixes=('', '_dup'))
                        else:
                            # Regular merge for other files (one record per call)
                            df = pd.merge(df, df_temp, on=common_cols, how='left', suffixes=('', '_dup'))
                        
                        # Handle duplicate columns by keeping the first occurrence
                        duplicate_cols = [col for col in df.columns if col.endswith('_dup')]
                        for dup_col in duplicate_cols:
                            original_col = dup_col.replace('_dup', '')
                            if original_col in df.columns:
                                # Combine values, preferring non-null values
                                df[original_col] = df[original_col].fillna(df[dup_col])
                                df = df.drop(columns=[dup_col])
                                
                    except Exception as e:
                        # If merge fails, just concatenate
                        df = pd.concat([df, df_temp], ignore_index=True, sort=False)
        
        # Clean column names
        df.columns = [str(c).strip() for c in df.columns]

        # Detect date column
        date_candidates = ['Call Date', 'Date', 'Visit Date', 'Activity Date', 'Created Date']
        date_col = next((c for c in df.columns if c in date_candidates), None)
        if date_col is None:
            like = [c for c in df.columns if 'date' in c.lower()]
            date_col = like[0] if like else None

        if date_col is not None:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df[~df[date_col].isna()].copy()
            df['Year'] = df[date_col].dt.year
            df['Month'] = df[date_col].dt.month
            df['MonthName'] = df[date_col].dt.strftime('%b')
            df['YearMonth'] = df[date_col].dt.to_period('M').dt.to_timestamp()

        # Sanitize textual columns to remove control characters
        df = sanitize_text_columns(df)

        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def identify_columns(df):
    """Identify relevant columns by common naming patterns."""
    lower = {c.lower(): c for c in df.columns}

    def find_one(cands):
        for cand in cands:
            if cand.lower() in lower:
                return lower[cand.lower()]
        for name in lower:
            for cand in cands:
                if cand.lower() in name:
                    return lower[name]
        return None

    return {
        'date': find_one(['Call Date', 'Date', 'Visit Date', 'Activity Date', 'Created Date']),
        'rep': find_one(['Rep', 'Rep Name', 'Sales Rep', 'Owner', 'AE', 'User']),
        'territory': find_one(['Territory', 'Region', 'Area']),
        'state': find_one(['State', 'Province']),
        'city': find_one(['City', 'Town']),
        'account': find_one(['Account', 'Account Name', 'Account: Name', 'Customer', 'Hospital', 'Clinic', 'Client']),
        'call_type': find_one(['Call Type', 'Type', 'Activity Type', 'Visit Type']),
        'outcome': find_one(['Outcome', 'Result', 'Status', 'Disposition']),
        'notes': find_one(['Notes', 'Call Notes', 'Description', 'Summary', 'Details', 'Comments']),
        'products': find_one(['Detailed Products', 'Products', 'Product(s)', 'Product', 'SKUs', 'Brands']),
        'attendees': find_one(['Attendees', 'Participants', 'Contacts Present', 'People Present', 'Contact Names', 'Attendee Names']),
        'pre_notes': find_one(['Pre-Call Notes', 'Pre Call Notes', 'Precall Notes', 'Pre-Visit Notes']),
        'post_notes': find_one(['Post-Call Notes', 'Post Call Notes', 'Postcall Notes', 'Follow-up Notes']),
        'discussion': find_one(['Call Discussion', 'Discussion', 'Call Discussions', 'Topics Discussed']),
        'detailed_notes': find_one(['Detailed Notes', 'Long Notes']),
        'key_messages': find_one(['Key Messages', 'Messages', 'Key Points', 'Main Messages', 'CLM Messages']),
        'clm_content': find_one(['CLM Content', 'CLM', 'Clinical Content', 'Clinical Information']),
        'next_steps': find_one(['Next Steps', 'Follow Up', 'Action Items', 'Next Actions']),
        'contact_info': find_one(['Contact Info', 'Contact Information', 'Contact Details', 'Phone', 'Email'])
    }

def sanitize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sanitize text columns: remove control chars and normalize whitespace."""
    cleaned = df.copy()
    control_chars = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
    backref_noise = re.compile(r"(?:\\\d\s*){3,}")  # sequences like \1\1\1...
    
    for col in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[col]):
            def _clean_cell(x):
                if isinstance(x, str):
                    s = control_chars.sub(" ", x)
                    s = backref_noise.sub(" ", s)
                    s = re.sub(r"\s+", " ", s).strip()
                    # normalize explicit 'nan' strings
                    if s.lower() in {"nan", "none", "null", "na", "n/a"}:
                        return None
                    return s
                return x
            cleaned[col] = cleaned[col].apply(_clean_cell)
    return cleaned

@st.cache_resource
def load_whisper_model():
    """Load Whisper model for speech-to-text (cached)."""
    try:
        return whisper.load_model("base")
    except Exception as e:
        st.error(f"Error loading Whisper model: {str(e)}")
        return None

def transcribe_audio(audio_bytes):
    """Transcribe audio to text using Whisper with soundfile (no FFmpeg needed)."""
    try:
        model = load_whisper_model()
        if model is None:
            st.error("❌ Whisper model could not be loaded")
            return None
        
        # Convert model to float32
        model = model.float()
        
        # Save audio bytes to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_file:
            tmp_file.write(audio_bytes)
            tmp_path = tmp_file.name
        
        try:
            # Read audio using soundfile (doesn't need FFmpeg)
            audio_data, sample_rate = sf.read(tmp_path)
            
            # Convert stereo to mono if needed
            if len(audio_data.shape) > 1:
                audio_data = audio_data.mean(axis=1)
            
            # Ensure float32 type
            audio_data = audio_data.astype(np.float32)
            
            # Transcribe
            result = model.transcribe(
                audio_data,
                fp16=False,
                verbose=False,
                language='en'
            )
            
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            
            return result["text"]
            
        except Exception as e:
            st.error(f"❌ Error during transcription: {str(e)}")
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            return None
            
    except Exception as e:
        st.error(f"❌ Error setting up transcription: {str(e)}")
        return None

def text_to_speech(text, rate=150, volume=0.8):
    """Convert text to speech and return audio data."""
    try:
        # Initialize TTS engine
        engine = pyttsx3.init()
        
        # Set properties
        engine.setProperty('rate', rate)  # Speed of speech
        engine.setProperty('volume', volume)  # Volume level (0.0 to 1.0)
        
        # Get available voices
        voices = engine.getProperty('voices')
        if voices:
            # Try to use a female voice if available
            for voice in voices:
                if 'female' in voice.name.lower() or 'woman' in voice.name.lower():
                    engine.setProperty('voice', voice.id)
                    break
        
        # Save audio to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as tmp_file:
            tmp_path = tmp_file.name
        
        # Generate speech and save to file
        engine.save_to_file(text, tmp_path)
        engine.runAndWait()
        
        # Read the audio file
        with open(tmp_path, 'rb') as audio_file:
            audio_data = audio_file.read()
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        return audio_data
    except Exception as e:
        st.error(f"Error generating speech: {str(e)}")
        return None

def play_audio_in_background(text):
    """Play text as audio in background thread."""
    def speak():
        try:
            engine = pyttsx3.init()
            engine.setProperty('rate', 150)
            engine.setProperty('volume', 0.8)
            
            # Get available voices
            voices = engine.getProperty('voices')
            if voices:
                for voice in voices:
                    if 'female' in voice.name.lower() or 'woman' in voice.name.lower():
                        engine.setProperty('voice', voice.id)
                        break
            
            # Store engine in session state for stopping
            st.session_state['tts_engine'] = engine
            
            engine.say(text)
            engine.runAndWait()
            
            # Clear engine from session state when done
            if 'tts_engine' in st.session_state:
                del st.session_state['tts_engine']
                
        except Exception as e:
            st.error(f"Error playing audio: {str(e)}")
            if 'tts_engine' in st.session_state:
                del st.session_state['tts_engine']
    
    # Start audio in background thread
    thread = threading.Thread(target=speak)
    thread.daemon = True
    thread.start()

def stop_audio():
    """Stop currently playing audio."""
    try:
        if 'tts_engine' in st.session_state:
            engine = st.session_state['tts_engine']
            engine.stop()
            del st.session_state['tts_engine']
            return True
        return False
    except Exception as e:
        st.error(f"Error stopping audio: {str(e)}")
        return False

def parse_voice_filters(voice_text, available_data):
    """Parse voice input and extract filter values using AI."""
    prompt = f"""
You are an AI assistant that extracts filter criteria from natural language instructions.

AVAILABLE DATA:
- Territories: {', '.join(available_data.get('territories', ['All'])[:20])}
- Sales Reps: {', '.join(available_data.get('reps', ['All'])[:20])}
- Call Types: {', '.join(available_data.get('call_types', ['All']))}
- Accounts: {', '.join(available_data.get('accounts', ['All'])[:30])}

USER INSTRUCTION:
"{voice_text}"

Extract filter values from the instruction. Return ONLY a valid JSON object with these fields:
{{
  "date_start": "YYYY-MM-DD or null",
  "date_end": "YYYY-MM-DD or null",
  "territory": "exact territory name or null",
  "sales_rep": "exact rep name or null",
  "call_type": "exact call type or null",
  "account": "exact account name or null"
}}

Rules:
- Use exact matches from the available data lists above
- If a filter is not mentioned, set it to null
- For dates, parse natural language like "from May 2nd to July 31st 2025" to proper format
- Return ONLY the JSON object, no other text
"""
    
    response = call_bi_llm(prompt, max_tokens=500, temperature=0.1)
    
    if response:
        try:
            # Extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                filters = json.loads(json_str)
                return filters
        except json.JSONDecodeError as e:
            st.error(f"Error parsing AI response: {str(e)}")
            st.text(f"Response was: {response}")
    
    return None

def get_bi_config():
    """Return BI LLM configuration."""
    return {
        'client_id': "b5f068d1-ff2d-4463-b7c4-a2b56bee532c",
        'client_secret': "b88f4db2-62ef-4405-bc19-ed15a5d4e4e4", 
        'model_name': "gpt-4.1",
        'token_url': "https://api-gw.boehringer-ingelheim.com:443/api/oauth/token",
        'api_url': "https://api-gw.boehringer-ingelheim.com:443/llm-api/",
        'temperature': 0.2,
        'max_tokens': 10000,
        'completions_path': 'chat/completions'
    }

@st.cache_data(show_spinner=False, ttl=2700)
def fetch_bi_token(client_id: str, client_secret: str, token_url: str):
    """Fetch OAuth2 token (client credentials); cache ~45 minutes."""
    try:
        resp = requests.post(
            token_url,
            data={'grant_type': 'client_credentials'},
            auth=(client_id, client_secret),
            timeout=20
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get('access_token')
        else:
            st.warning(f"Token fetch failed with status: {resp.status_code}")
            st.error(f"Response: {resp.text}")
    except Exception as e:
        st.error(f"Token fetch error: {str(e)}")
    return None

def call_bi_llm(prompt: str, max_tokens: int = None, temperature: float = None):
    """Call BI LLM API and extract response text."""
    cfg = get_bi_config()
    
    # Get token
    token = fetch_bi_token(cfg['client_id'], cfg['client_secret'], cfg['token_url'])
    if not token:
        st.error("Failed to obtain authentication token")
        return None
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'model': cfg['model_name'],
        'messages': [{'role': 'user', 'content': prompt}],
        'temperature': temperature if temperature is not None else cfg['temperature'],
        'max_tokens': max_tokens if max_tokens is not None else cfg['max_tokens']
    }
    
    # Construct full URL
    base_url = cfg['api_url'].rstrip('/')
    completions_path = cfg['completions_path'].lstrip('/')
    full_url = f"{base_url}/{completions_path}"
    
    try:
        with st.spinner("Calling BI LLM API..."):
            response = requests.post(full_url, headers=headers, json=payload, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            
            # Try to extract content from different possible response formats
            if isinstance(data, dict):
                # OpenAI-style response
                if 'choices' in data and data['choices']:
                    choice = data['choices'][0]
                    if isinstance(choice, dict):
                        if 'message' in choice and 'content' in choice['message']:
                            return choice['message']['content']
                        if 'text' in choice:
                            return choice['text']
                
                # Anthropic-style response
                if 'content' in data and isinstance(data['content'], list) and data['content']:
                    content_item = data['content'][0]
                    if isinstance(content_item, dict) and 'text' in content_item:
                        return content_item['text']
                
                # Other possible formats
                for key in ['output', 'output_text', 'result', 'response']:
                    if key in data and isinstance(data[key], str):
                        return data[key]
                
                # If it's a direct string response
                if isinstance(data, str):
                    return data
            
            st.error(f"Unexpected response format. Available keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            return None
        
        else:
            st.error(f"API call failed with status {response.status_code}")
            st.error(f"Response: {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        st.error("API call timed out (60 seconds)")
        return None
    except requests.exceptions.ConnectionError:
        st.error("Connection error - could not reach API endpoint")
        return None
    except Exception as e:
        st.error(f"Unexpected error calling API: {str(e)}")
        return None

def generate_with_ai(prompt: str, max_tokens: int = None, temperature: float = None):
    """Generate AI response with anti-repetition measures."""
    # Add uniqueness to prevent caching/repetition
    timestamp = str(int(time.time()))
    prompt_hash = hashlib.md5(prompt.encode()).hexdigest()[:8]
    
    # Enhance prompt with unique elements and variety instructions
    enhanced_prompt = f"""{prompt}

[Analysis ID: {timestamp}-{prompt_hash}]
[Instructions: Provide a fresh, detailed, and unique analysis. Vary your language, structure, and insights. Avoid repetitive phrasing or generic responses. Focus on specific, actionable details unique to this situation.]"""

    return call_bi_llm(enhanced_prompt, max_tokens=max_tokens, temperature=temperature)

def format_filters_for_display(filters_applied):
    """Format applied filters for display."""
    if not filters_applied:
        return "All data (no filters applied)"
    return " | ".join(filters_applied)

def create_call_charts(df, cols):
    """Create Plotly charts for call data including CLM analytics."""
    if df.empty or not cols.get('date'):
        return None, None, None, None

    # Calls per month (count)
    monthly = df.groupby(['Year', 'Month', 'YearMonth']).size().reset_index(name='Calls')
    monthly = monthly.sort_values('YearMonth')
    palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    color_map = {year: palette[i % len(palette)] for i, year in enumerate(sorted(monthly['Year'].unique()))}

    fig1 = go.Figure()
    for year in monthly['Year'].unique():
        year_data = monthly[monthly['Year'] == year]
        fig1.add_trace(go.Bar(x=year_data['Month'], y=year_data['Calls'], name=str(year), marker_color=color_map.get(year)))
    fig1.update_layout(
        title="Calls by Month", 
        xaxis_title="Month", 
        yaxis_title="# Calls", 
        height=300,
        margin=dict(l=20, r=20, t=40, b=20), 
        font=dict(size=10), 
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    # Outcome distribution (if exists)
    fig2 = None
    if cols.get('outcome') and not df[cols['outcome']].isna().all():
        outcome_counts = df[cols['outcome']].fillna('Unknown').value_counts().reset_index()
        outcome_counts.columns = ['Outcome', 'Count']
        fig2 = go.Figure(data=[go.Pie(labels=outcome_counts['Outcome'], values=outcome_counts['Count'], hole=0.45)])
        fig2.update_layout(title="Outcome Distribution", height=300, margin=dict(l=20, r=20, t=40, b=20))

    # CLM Key Messages Usage
    fig3 = None
    key_message_col = None
    for col in df.columns:
        if 'key_message' in col.lower() or 'keymsg_name' in col.lower():
            key_message_col = col
            break
    
    if key_message_col and not df[key_message_col].isna().all():
        # Split and count key messages
        all_messages = []
        for messages in df[key_message_col].dropna():
            if isinstance(messages, str):
                # Split by semicolon and clean up
                msg_list = [msg.strip() for msg in messages.split(';') if msg.strip()]
                all_messages.extend(msg_list)
        
        if all_messages:
            message_counts = pd.Series(all_messages).value_counts().head(10)  # Top 10 messages
            fig3 = go.Figure(data=[go.Bar(
                x=message_counts.values,
                y=message_counts.index,
                orientation='h',
                marker_color='#2ca02c'
            )])
            fig3.update_layout(
                title="Top CLM Key Messages Used",
                xaxis_title="Usage Count",
                yaxis_title="Key Messages",
                height=400,
                margin=dict(l=20, r=20, t=40, b=20)
            )

    # CLM Duration Analysis
    fig4 = None
    duration_col = None
    for col in df.columns:
        if 'clm' in col.lower() and 'duration' in col.lower():
            duration_col = col
            break
    
    if duration_col and not df[duration_col].isna().all():
        # Convert to numeric and filter valid durations
        durations = pd.to_numeric(df[duration_col], errors='coerce').dropna()
        durations = durations[durations > 0]  # Remove zero or negative durations
        
        if not durations.empty:
            # Create duration bins
            bins = [0, 30, 60, 120, 300, float('inf')]
            labels = ['0-30s', '31-60s', '1-2min', '2-5min', '5min+']
            duration_categories = pd.cut(durations, bins=bins, labels=labels, right=False)
            
            duration_counts = duration_categories.value_counts()
            fig4 = go.Figure(data=[go.Bar(
                x=duration_counts.index,
                y=duration_counts.values,
                marker_color='#d62728'
            )])
            fig4.update_layout(
                title="CLM Content Duration Distribution",
                xaxis_title="Duration Range",
                yaxis_title="Number of Calls",
                height=300,
                margin=dict(l=20, r=20, t=40, b=20)
            )

    return fig1, fig2, fig3, fig4

def get_meeting_summary_and_next_insight(df, cols, account, start_date, end_date, filters_applied):
    """Generate meeting summary and next-step analysis via AI."""
    # Build context from the specified period
    dcol = cols.get('date')
    if not dcol or not cols.get('account'):
        return "No date/account columns detected."

    mask = (df[cols['account']] == account)
    if start_date and end_date:
        mask &= (pd.to_datetime(df[dcol]).dt.date >= start_date) & (pd.to_datetime(df[dcol]).dt.date <= end_date)
    period_df = df[mask].copy().sort_values(dcol)

    # Build meeting summaries
    summaries = []
    fields = {
        'Products': cols.get('products'),
        'Attendees': cols.get('attendees'),
        'Pre-Call Notes': cols.get('pre_notes'),
        'Discussion': cols.get('discussion'),
        'Post-Call Notes': cols.get('post_notes'),
        'General Notes': cols.get('notes'),
        'Detailed Notes': cols.get('detailed_notes'),
        'Key Messages': cols.get('key_messages'),
        'CLM Content': cols.get('clm_content'),
        'Next Steps': cols.get('next_steps'),
        'Contact Info': cols.get('contact_info')
    }

    for _, r in period_df.iterrows():
        parts = []
        dt = r[dcol]
        try:
            date_str = pd.to_datetime(dt).strftime('%Y-%m-%d')
        except Exception:
            date_str = str(dt)
        parts.append(f"**Date:** {date_str}")
        
        # Add other fields if available
        if cols.get('rep') and pd.notna(r.get(cols['rep'])):
            parts.append(f"**Rep:** {r[cols['rep']]}")
        if cols.get('call_type') and pd.notna(r.get(cols['call_type'])):
            parts.append(f"**Type:** {r[cols['call_type']]}")
        if cols.get('outcome') and pd.notna(r.get(cols['outcome'])):
            parts.append(f"**Outcome:** {r[cols['outcome']]}")
        
        # Add CLM information if available
        clm_duration_col = None
        for col in r.index:
            if 'clm' in col.lower() and 'duration' in col.lower():
                clm_duration_col = col
                break
        
        if clm_duration_col and pd.notna(r.get(clm_duration_col)):
            clm_duration = r[clm_duration_col]
            if str(clm_duration).strip() and str(clm_duration).lower() not in {"nan", "none", "null", "na", "n/a"}:
                parts.append(f"**CLM Duration:** {clm_duration} seconds")
        
        # Process all note fields
        for label, col in fields.items():
            if col and col in r and pd.notna(r[col]) and str(r[col]).strip():
                val = str(r[col]).strip()
                if val.lower() not in {"nan", "none", "null", "na", "n/a"}:
                    parts.append(f"**{label}:** {val}")
        
        summaries.append(" | ".join(parts))

    period_summary = "\n\n".join(summaries) if summaries else "No meetings found in the specified period."
    
    # Enhanced prompt for AI analysis
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    unique_id = hashlib.md5(f"{account}{start_date}{end_date}{current_time}".encode()).hexdigest()[:12]
    
    prompt = f"""
ROLE: You are a senior pharmaceutical/veterinary sales strategist with deep expertise in relationship management and deal progression.

ANALYSIS CONTEXT:
- Analysis ID: {unique_id}
- Target Account: {account}
- Analysis Period: {start_date} to {end_date}
- Data Scope: {format_filters_for_display(filters_applied)}
- Current Date: {current_time}

MEETING DATA TO ANALYZE:
{period_summary}

OUTPUT REQUIREMENTS:

**PERIOD OVERVIEW**
- Key relationship developments during this period

**MEETING ANALYSIS**
- Summarize each meeting with key insights
- Identify patterns and progression in discussions
- Note customer priorities and concerns raised
- Analyze CLM (Closed Loop Marketing) usage: duration, key messages presented, and engagement levels

**STRATEGIC INTERPRETATION**
- What the interaction patterns reveal about customer intent
- Key success factors and potential obstacles
- Recommendations for advancing the relationship
- How CLM content and key messages are influencing the customer journey

**NEXT MEETING PLAN**
- 2-3 specific agenda items with evidence-based rationale
- Each item should reference specific interactions
- Suggest relevant CLM content and key messages for the next interaction
- Focus on advancing the sales process


Base all recommendations on specific evidence from the meeting data. Avoid generic advice.
"""

    text = generate_with_ai(prompt, max_tokens=1000, temperature=0.6)
    if isinstance(text, str) and text.strip():
        return text
    
    # Fallback if AI is not available
    return f"""
**PERIOD OVERVIEW**
Account: {account}
Period: {start_date} to {end_date}
Scope: {format_filters_for_display(filters_applied)}

**MEETINGS FOUND**
{period_summary}

**RECOMMENDATIONS**
- Review meeting history for patterns and opportunities
- Address any unresolved issues or concerns
- Prepare materials aligned with customer priorities
- Schedule next meeting with specific objectives
"""

def format_ai_response(text):
    """Format AI response text into styled HTML."""
    # Clean up any remaining artifacts
    text = re.sub(r"(?:\\\d\s*){3,}", " ", text)
    text = re.sub(r"\[Analysis ID:.*?\]", "", text)  # Remove analysis IDs from display
    
    lines = text.split('\n')
    formatted = []
    
    for line in lines:
        line = line.strip()
        if not line:
            formatted.append('<br>')
        elif line.startswith('**') and line.endswith('**') and len(line) > 4:
            # Main headers
            header_text = line.replace('**', '')
            formatted.append(f'<h5 style="color: #1f77b4; margin-top: 15px; margin-bottom: 8px;">{header_text}</h5>')
        elif line.startswith('**') and '**' in line[2:]:
            # Bold text within content
            formatted_line = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', line)
            formatted.append(f'<p style="margin: 5px 0;">{formatted_line}</p>')
        elif line.startswith('- ') or line.startswith('• '):
            # Bullet points
            bullet_text = line[2:].strip()
            formatted.append(f'<p style="margin: 3px 0 3px 20px;">• {bullet_text}</p>')
        else:
            formatted.append(f'<p style="margin: 5px 0;">{line}</p>')
    
    return ''.join(formatted)

def main():
    """Render Streamlit UI and interactions."""
    # Header with improved styling
    st.markdown("""
    <div class="header-container">
        <h3 class="centered-title" style="color: #1f77b4;">
            <b>AI Call Assistant — Strategic Visit Preparation</b>
        </h3>
    </div>
    """, unsafe_allow_html=True)

    # Enhanced sidebar
    st.sidebar.header("Analysis Controls")

    # Data source with better messaging
    uploaded = st.sidebar.file_uploader(
        "Upload Call History", 
        type=["xlsx"], 
        help="Upload your call notes Excel file (same format as CRM export)"
    )
    
    if uploaded is not None:
        df = load_data(file_bytes=uploaded.getvalue(), filename=uploaded.name)
        st.sidebar.success(f"Loaded: {uploaded.name}")
        st.sidebar.caption(f"Records: {len(df) if df is not None else 0}")
    else:
        df = load_data()
        if df is not None:
            pass
    
    if df is None or df.empty:
        st.error("No data available. Please upload a valid Excel file or ensure the default file exists.")
        st.stop()

    cols = identify_columns(df)
    
    # Display column mapping for transparency
    with st.sidebar.expander("Column Mapping", expanded=False):
        for key, value in cols.items():
            if value:
                st.text(f"{key}: {value}")

    # Logo with better positioning
    logo_path = "BI-Logo.png"
    logo = load_logo(logo_path, width=80)
    if logo is not None:
        st.sidebar.image(logo, use_container_width=True)

    # Enhanced filters with better organization
    st.sidebar.subheader("Filters")
    
    # Voice Input Section
    st.sidebar.markdown("---")
    st.sidebar.markdown("🎤 **Voice Input**")
    st.sidebar.caption("Click to record voice instructions")
    
    # Simple microphone button with auto-stop
    with st.sidebar:
        audio_bytes = audio_recorder(
            text="🎤 Record Voice",
            recording_color="#ff0000",
            neutral_color="#1f77b4",
            icon_name="microphone",
            icon_size="2x",
            key="voice_recorder",
            pause_threshold=5.0,  # Auto-stop after 5 seconds of silence
            sample_rate=16000     # Optimized for speech recognition
        )
    
    # Process recorded audio
    if audio_bytes:
        if 'processing_started' not in st.session_state:
            st.session_state['processing_started'] = True
            
            st.sidebar.info("🔄 Transcribing audio...")
            
            # Transcribe audio
            transcript = transcribe_audio(audio_bytes)
            
            if transcript:
                st.sidebar.success("✓ Audio transcribed")
                with st.sidebar.expander("View Transcript", expanded=True):
                    st.write(transcript)
                
                # Get available filter options
                available_data = {
                    'territories': df[cols['territory']].dropna().unique().tolist() if cols.get('territory') else [],
                    'reps': df[cols['rep']].dropna().unique().tolist() if cols.get('rep') else [],
                    'call_types': df[cols['call_type']].dropna().unique().tolist() if cols.get('call_type') else [],
                    'accounts': df[cols['account']].dropna().unique().tolist() if cols.get('account') else []
                }
                
                # Parse filters from transcript
                st.sidebar.info("🤖 Analyzing your request...")
                extracted_filters = parse_voice_filters(transcript, available_data)
                
                if extracted_filters:
                    st.sidebar.success("✓ Filters extracted")
                    st.session_state['voice_filters'] = extracted_filters
                    st.session_state['voice_transcript'] = transcript
                else:
                    st.sidebar.error("Could not extract filters from voice input")
            else:
                st.sidebar.error("Failed to transcribe audio. Please try again.")
            
            # Clean up
            if 'processing_started' in st.session_state:
                del st.session_state['processing_started']
    
    # Display extracted filters if available
    if 'voice_filters' in st.session_state:
        with st.sidebar.expander("🎯 Voice Filter Summary", expanded=True):
            voice_filters = st.session_state['voice_filters']
            st.write("**Extracted from voice:**")
            for key, value in voice_filters.items():
                if value and value != "null":
                    st.write(f"• {key}: {value}")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Apply Filters", type="primary", use_container_width=True, key="apply_voice"):
                    st.session_state['apply_voice_filters'] = True
                    st.rerun()
            with col2:
                if st.button("🔄 Clear", use_container_width=True, key="clear_voice"):
                    # Clear all voice-related session state
                    if 'voice_filters' in st.session_state:
                        del st.session_state['voice_filters']
                    if 'voice_transcript' in st.session_state:
                        del st.session_state['voice_transcript']
                    if 'apply_voice_filters' in st.session_state:
                        del st.session_state['apply_voice_filters']
                    st.rerun()
    
    st.sidebar.markdown("---")
    
    filters_applied = []
    filtered_df = df.copy()

    # Date filter with voice filter integration
    start = None
    end = None
    if cols.get('date'):
        min_d = pd.to_datetime(filtered_df[cols['date']]).min()
        max_d = pd.to_datetime(filtered_df[cols['date']]).max()
        
        # Default to last 90 days if data spans more than 90 days
        default_start = max_d - pd.Timedelta(days=90) if (max_d - min_d).days > 90 else min_d
        default_end = max_d.date()
        
        # Apply voice filters if available
        voice_start = None
        voice_end = None
        if st.session_state.get('apply_voice_filters') and 'voice_filters' in st.session_state:
            voice_filters = st.session_state['voice_filters']
            if voice_filters.get('date_start') and voice_filters['date_start'] != 'null':
                try:
                    voice_start = pd.to_datetime(voice_filters['date_start']).date()
                except:
                    pass
            if voice_filters.get('date_end') and voice_filters['date_end'] != 'null':
                try:
                    voice_end = pd.to_datetime(voice_filters['date_end']).date()
                except:
                    pass
        
        # Use voice filter dates if available, otherwise use defaults
        if voice_start and voice_end:
            date_range_value = (voice_start, voice_end)
        else:
            date_range_value = (default_start.date(), default_end)
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=date_range_value,
            min_value=min_d.date(),
            max_value=max_d.date()
        )
        
        if len(date_range) == 2:
            start, end = date_range
            mask = (pd.to_datetime(filtered_df[cols['date']]).dt.date >= start) & (pd.to_datetime(filtered_df[cols['date']]).dt.date <= end)
            filtered_df = filtered_df[mask]
            filters_applied.append(f"{start} → {end}")

    # Geographic filters
    if cols.get('territory'):
        terrs = ['All'] + sorted([x for x in filtered_df[cols['territory']].dropna().unique().tolist()])
        
        # Apply voice filter for territory if available
        voice_territory = None
        if st.session_state.get('apply_voice_filters') and 'voice_filters' in st.session_state:
            voice_filters = st.session_state['voice_filters']
            if voice_filters.get('territory') and voice_filters['territory'] != 'null':
                voice_territory = voice_filters['territory']
        
        # Set default selection based on voice filter
        default_territory = voice_territory if voice_territory and voice_territory in terrs else 'All'
        territory_index = terrs.index(default_territory) if default_territory in terrs else 0
        
        selected_territory = st.sidebar.selectbox("Territory", terrs, index=territory_index)
        if selected_territory != 'All':
            filtered_df = filtered_df[filtered_df[cols['territory']] == selected_territory]
            filters_applied.append(f"Territory: {selected_territory}")

    if cols.get('state'):
        states = ['All'] + sorted([x for x in filtered_df[cols['state']].dropna().unique().tolist()])
        selected_state = st.sidebar.selectbox("State", states)
        if selected_state != 'All':
            filtered_df = filtered_df[filtered_df[cols['state']] == selected_state]
            filters_applied.append(f"State: {selected_state}")

    if cols.get('city'):
        cities = ['All'] + sorted([x for x in filtered_df[cols['city']].dropna().unique().tolist()])
        selected_city = st.sidebar.selectbox("City", cities)
        if selected_city != 'All':
            filtered_df = filtered_df[filtered_df[cols['city']] == selected_city]
            filters_applied.append(f"City: {selected_city}")

    # People filters
    if cols.get('rep'):
        reps = ['All'] + sorted([x for x in filtered_df[cols['rep']].dropna().unique().tolist()])
        
        # Apply voice filter for sales rep if available
        voice_rep = None
        if st.session_state.get('apply_voice_filters') and 'voice_filters' in st.session_state:
            voice_filters = st.session_state['voice_filters']
            if voice_filters.get('sales_rep') and voice_filters['sales_rep'] != 'null':
                voice_rep = voice_filters['sales_rep']
        
        # Set default selection based on voice filter
        default_rep = voice_rep if voice_rep and voice_rep in reps else 'All'
        rep_index = reps.index(default_rep) if default_rep in reps else 0
        
        selected_rep = st.sidebar.selectbox("Sales Rep", reps, index=rep_index)
        if selected_rep != 'All':
            filtered_df = filtered_df[filtered_df[cols['rep']] == selected_rep]
            filters_applied.append(f"Rep: {selected_rep}")

    if cols.get('call_type'):
        ctypes = ['All'] + sorted([x for x in filtered_df[cols['call_type']].dropna().unique().tolist()])
        
        # Apply voice filter for call type if available
        voice_call_type = None
        if st.session_state.get('apply_voice_filters') and 'voice_filters' in st.session_state:
            voice_filters = st.session_state['voice_filters']
            if voice_filters.get('call_type') and voice_filters['call_type'] != 'null':
                voice_call_type = voice_filters['call_type']
        
        # Set default selection based on voice filter
        default_call_type = voice_call_type if voice_call_type and voice_call_type in ctypes else 'All'
        call_type_index = ctypes.index(default_call_type) if default_call_type in ctypes else 0
        
        selected_ctype = st.sidebar.selectbox("Call Type", ctypes, index=call_type_index)
        if selected_ctype != 'All':
            filtered_df = filtered_df[filtered_df[cols['call_type']] == selected_ctype]
            filters_applied.append(f"Type: {selected_ctype}")

    # Account filter with search capability and voice filter integration
    selected_account = None
    if cols.get('account'):
        accts = sorted([x for x in filtered_df[cols['account']].dropna().unique().tolist()])
        if accts:
            # Apply voice filter for account if available
            voice_account = None
            if st.session_state.get('apply_voice_filters') and 'voice_filters' in st.session_state:
                voice_filters = st.session_state['voice_filters']
                if voice_filters.get('account') and voice_filters['account'] != 'null':
                    voice_account = voice_filters['account']
            
            # Add search functionality for accounts
            search_term = st.sidebar.text_input("Search Accounts", placeholder="Type to search...")
            if search_term:
                filtered_accts = [acc for acc in accts if search_term.lower() in acc.lower()]
                if filtered_accts:
                    # Set default based on voice filter
                    default_account = voice_account if voice_account and voice_account in filtered_accts else 'All'
                    account_options = ['All'] + filtered_accts
                    account_index = account_options.index(default_account) if default_account in account_options else 0
                    selected_account = st.sidebar.selectbox("Select Account", account_options, index=account_index)
                else:
                    st.sidebar.warning("No accounts match your search")
                    selected_account = st.sidebar.selectbox("All Accounts", ['All'] + accts)
            else:
                # Set default based on voice filter
                default_account = voice_account if voice_account and voice_account in accts else 'All'
                account_options = ['All'] + accts
                account_index = account_options.index(default_account) if default_account in account_options else 0
                selected_account = st.sidebar.selectbox("Account", account_options, index=account_index)
            
            if selected_account != 'All':
                filtered_df = filtered_df[filtered_df[cols['account']] == selected_account]
                filters_applied.append(f"Account: {selected_account}")

    # Clear cache button
    if st.sidebar.button("Clear Cache & Refresh", help="Clear AI cache and refresh data"):
        st.cache_data.clear()
        for key in list(st.session_state.keys()):
            if key.startswith('meeting_'):
                del st.session_state[key]
        st.rerun()

    # API status check
    with st.sidebar.expander("API Status", expanded=False):
        cfg = get_bi_config()
        st.text(f"Model: {cfg['model_name']}")
        st.text(f"Max Tokens: {cfg['max_tokens']}")
        st.text(f"Temperature: {cfg['temperature']}")
        
        if st.button("Test API Connection"):
            test_prompt = "Hello, this is a test. Please respond with 'API connection successful'."
            response = call_bi_llm(test_prompt, max_tokens=50, temperature=0.1)
            if response:
                st.success("API connection successful!")
            else:
                st.error("API connection failed")

    # Main content layout
    col1, col2 = st.columns([1.3, 0.7])

    with col1:
        
        # Data summary metrics
        if not filtered_df.empty:
            metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
            with metrics_col1:
                st.metric("Total Calls", len(filtered_df))
            with metrics_col2:
                if cols.get('account'):
                    st.metric("Unique Accounts", filtered_df[cols['account']].nunique())
            with metrics_col3:
                if cols.get('rep'):
                    st.metric("Active Reps", filtered_df[cols['rep']].nunique())
            with metrics_col4:
                if cols.get('date'):
                    days_span = (filtered_df[cols['date']].max() - filtered_df[cols['date']].min()).days
                    st.metric("Days Span", days_span)
            
            # CLM Metrics
            st.subheader("📈 CLM Usage Statistics")
            
            # Find CLM columns
            duration_col = None
            key_message_col = None
            
            for col in filtered_df.columns:
                if 'clm' in col.lower() and 'duration' in col.lower():
                    duration_col = col
                elif 'key_message' in col.lower() or 'keymsg_name' in col.lower():
                    key_message_col = col
            
            clm_metrics_col1, clm_metrics_col2, clm_metrics_col3, clm_metrics_col4 = st.columns(4)
            
            with clm_metrics_col1:
                # CLM Usage Rate
                if duration_col:
                    clm_used_calls = filtered_df[duration_col].notna().sum()
                    total_calls = len(filtered_df)
                    usage_rate = (clm_used_calls / total_calls * 100) if total_calls > 0 else 0
                    st.metric("CLM Usage Rate", f"{usage_rate:.1f}%", f"{clm_used_calls}/{total_calls} calls")
                else:
                    st.metric("CLM Usage Rate", "N/A")
            
            with clm_metrics_col2:
                # Average CLM Duration
                if duration_col:
                    durations = pd.to_numeric(filtered_df[duration_col], errors='coerce').dropna()
                    durations = durations[durations > 0]
                    if not durations.empty:
                        avg_duration = durations.mean()
                        st.metric("Avg CLM Duration", f"{avg_duration:.1f}s", f"Range: {durations.min():.0f}-{durations.max():.0f}s")
                    else:
                        st.metric("Avg CLM Duration", "No data")
                else:
                    st.metric("Avg CLM Duration", "N/A")
            
            with clm_metrics_col3:
                # Unique Key Messages
                if key_message_col:
                    all_messages = []
                    for messages in filtered_df[key_message_col].dropna():
                        if isinstance(messages, str):
                            msg_list = [msg.strip() for msg in messages.split(';') if msg.strip()]
                            all_messages.extend(msg_list)
                    unique_messages = len(set(all_messages)) if all_messages else 0
                    st.metric("Unique Key Messages", unique_messages, f"{len(all_messages)} total uses")
                else:
                    st.metric("Unique Key Messages", "N/A")
            
            with clm_metrics_col4:
                # Most Used Key Message
                if key_message_col:
                    all_messages = []
                    for messages in filtered_df[key_message_col].dropna():
                        if isinstance(messages, str):
                            msg_list = [msg.strip() for msg in messages.split(';') if msg.strip()]
                            all_messages.extend(msg_list)
                    if all_messages:
                        message_counts = pd.Series(all_messages).value_counts()
                        top_message = message_counts.index[0]
                        top_count = message_counts.iloc[0]
                        st.metric("Most Used Message", f"{top_count} uses", top_message[:30] + "..." if len(top_message) > 30 else top_message)
                    else:
                        st.metric("Most Used Message", "No data")
                else:
                    st.metric("Most Used Message", "N/A")

        # Enhanced data table
        st.subheader("Call History Details")
        table_df = filtered_df.copy()
        if cols.get('date'):
            table_df = table_df.sort_values(cols['date'], ascending=False)
        
        st.dataframe(
            table_df, 
            use_container_width=True, 
            height=400,
            column_config={
                cols.get('date'): st.column_config.DatetimeColumn(
                    "Date",
                    format="YYYY-MM-DD"
                ) if cols.get('date') else None
            }
        )

    with col2:
        # Enhanced AI Assistant section
        st.subheader("AI Strategic Assistant")
        
        # Analysis scope display
        scope_display = format_filters_for_display(filters_applied)
        st.info(f"**Analysis Scope:** {scope_display}")
        
        # Use account from sidebar filter
        target_account = selected_account

        # Enhanced AI analysis buttons
        if target_account and cols.get('account') and cols.get('date'):
            
            # Period summary button
            generate_period_summary = st.button(
                "Generate Strategic Analysis", 
                type="primary", 
                use_container_width=True,
                help="Analyze the selected period and generate strategic insights for the next visit"
            )
            
            if generate_period_summary:
                if not (start and end):
                    st.warning("Please set a valid date range for analysis.")
                else:
                    with st.spinner("Generating comprehensive strategic analysis..."):
                        # Use a unique key for this analysis
                        analysis_key = f"meeting_summary_{target_account}_{start}_{end}_{hash(str(filters_applied))}"
                        
                        combined = get_meeting_summary_and_next_insight(
                            df, cols, target_account, start, end, filters_applied
                        )
                        if combined:
                            st.session_state[analysis_key] = combined
                        else:
                            st.error("Failed to generate analysis. Please check API connection.")
            
            # Display results if available
            analysis_key = f"meeting_summary_{target_account}_{start}_{end}_{hash(str(filters_applied))}"
            if analysis_key in st.session_state:
                st.markdown(f"""
                <div class="insight-container">
                    <h4>Strategic Analysis: {target_account}</h4>
                    {format_ai_response(st.session_state[analysis_key])}
                </div>
                """, unsafe_allow_html=True)
                
                # Action buttons
                col_clear, col_export, col_audio, col_stop = st.columns(4)
                with col_clear:
                    if st.button("Clear Analysis", use_container_width=True):
                        del st.session_state[analysis_key]
                        st.rerun()
                
                with col_export:
                    if st.button("Show Raw Text", use_container_width=True):
                        st.text_area("Copy this text:", st.session_state[analysis_key], height=200)
                
                with col_audio:
                    if st.button("🔊 Play Audio", use_container_width=True, help="Listen to the analysis"):
                        # Clean text for better speech
                        clean_text = st.session_state[analysis_key]
                        # Remove markdown formatting for cleaner speech
                        clean_text = re.sub(r'\*\*(.*?)\*\*', r'\1', clean_text)  # Remove bold
                        clean_text = re.sub(r'_(.*?)_', r'\1', clean_text)  # Remove italic
                        clean_text = re.sub(r'`(.*?)`', r'\1', clean_text)  # Remove code
                        clean_text = re.sub(r'#{1,6}\s*', '', clean_text)  # Remove headers
                        clean_text = re.sub(r'[•\-\*]\s*', '', clean_text)  # Remove bullet points
                        clean_text = re.sub(r'\n+', ' ', clean_text)  # Replace newlines with spaces
                        clean_text = re.sub(r'\s+', ' ', clean_text)  # Clean up extra spaces
                        
                        # Play audio in background
                        play_audio_in_background(clean_text)
                        st.success("🎵 Playing audio analysis...")
                
                with col_stop:
                    if st.button("⏹️ Stop Audio", use_container_width=True, help="Stop currently playing audio"):
                        if stop_audio():
                            st.success("🔇 Audio stopped")
                        else:
                            st.info("ℹ️ No audio currently playing")
        else:
            st.warning("Please select an account and ensure date data is available for AI analysis.")

        # Quick insights panel
        if not filtered_df.empty:
            with st.expander("Quick Insights", expanded=True):
                if cols.get('account'):
                    top_accounts = filtered_df[cols['account']].value_counts().head(5)
                    st.write("**Most Active Accounts:**")
                    for acc, count in top_accounts.items():
                        st.write(f"• {acc}: {count} calls")
                
                if cols.get('outcome') and not filtered_df[cols['outcome']].isna().all():
                    st.write("**Recent Outcomes:**")
                    outcomes = filtered_df[cols['outcome']].value_counts().head(3)
                    for outcome, count in outcomes.items():
                        st.write(f"• {outcome}: {count}")

if __name__ == "__main__":
    main()