from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import csv
from email import message
import os.path
import pickle
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
MAIL_REGEX = r"[\w\.\+\-\_]+@[\w\.\-\_]+\.[\w\d]+"

class GmailClient:
  creds = None
  gmail_service = None

  def __init__(self):
    if os.path.exists('token.json'):
      self.creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not self.creds or not self.creds.valid:
      if self.creds and self.creds.expired and self.creds.refresh_token:
        self.creds.refresh(Request())
      else:
        flow = InstalledAppFlow.from_client_secrets_file('creds.json', scopes=SCOPES)
        self.creds = flow.run_local_server(port = 0)

      # Save the credentials for the next run
      with open('token.json', 'w') as token:
        token.write(self.creds.to_json())

    try:
      # Call the Gmail API
      self.gmail_service = build('gmail', 'v1', credentials=self.creds)
    except HttpError as error:
      print(f"An error ocurred: {error}")

  def GetMessages(self, user_id, page_token=None):
    return self.gmail_service.users().messages().list(userId=user_id, pageToken=page_token).execute()

  def GetMessage(self, user_id, message_id):
    if (not message_id):
      print("meesage_id can't be empty")
      return
    return self.gmail_service.users().messages().get(userId=user_id, id=message_id).execute()

  def GetMessageSenderInternal(message):
    payload = message.get('payload', None)
    if (not payload):
      print("No payload")
      return
    for header in payload.get('headers', []):
      is_from_header = header.get('name', '').lower() == "from"
      if is_from_header:
        from_address = header.get('value', None)
        from_address = from_address.replace('"', '')
        from_address = from_address.encode("UTF-8", errors='ignore')
        return from_address
    print("No from address found")
    print(payload)
    return "Unknown".encode("UTF-8", errors='ignore')

def GetMessageSenderAndStatus(user_id, message_id):
  tmp_gmail_client = GmailClient()
  if (user_id is None or message_id is None):
    print("user_id and message_id must not be none")
    return ""
  message = tmp_gmail_client.GetMessage(user_id, message_id)
  senderRaw = GmailClient.GetMessageSenderInternal(message)
  senderReg = re.search(MAIL_REGEX, senderRaw.decode('Utf-8'))
  sender = "unknown"
  if senderReg is None:
    print("ERROR extracting mail")
    print(senderRaw.decode('utf8'))
  else:
    sender = senderReg.group(0)
  unread = False
  if "UNREAD" in message.get('labelIds', []):
    unread = True
  return {"sender": sender, "unread": unread, "id": message.get('id', '')}


def main():
  """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
  """
  gmail_client = GmailClient()
  next_page_token = None
  all_messages = set()
  messages_max_size_500_bucket = 0
  while(True):
    current_bucket = len(all_messages) // 500
    if current_bucket > messages_max_size_500_bucket:
      print(f"total messages: {len(all_messages)}")
      messages_max_size_500_bucket = current_bucket

    result = gmail_client.GetMessages('me', next_page_token)
    next_page_token = result.get('nextPageToken', None)
    new_messages_obj = result.get('messages', [])

    if (next_page_token is None or len(new_messages_obj) == 0):
      break

    for message in new_messages_obj:
      all_messages.add(message.get("id", ""))

  read_messages = set()
  if os.path.exists("read_messages.bin"):
    with open('read_messages.bin', 'rb') as read_messages_file:
      read_messages = pickle.load(read_messages_file)

  with open('all_messages.bin', 'wb') as all_messages_file:
    pickle.dump(all_messages, all_messages_file)
    
  senders = {}
  sender_count = 0

  unread_messages = all_messages.difference(read_messages)
  with ThreadPoolExecutor(max_workers=10) as executor:
    futures_response = {executor.submit(GetMessageSenderAndStatus, "me", message_id): message_id for message_id in unread_messages}
    for future in as_completed(futures_response):
      try:
        response = future.result()
        if (response.get('unread', False)):
          sender_count += 1
          sender = response.get('sender', '')
          if (sender_count % 1000 == 0):
            print(sender_count)
          if (sender not in senders):
            senders[sender] = 1
          else:
            senders[sender] = senders[sender] + 1
        else:
          read_messages.add(response.get('id', ''))
      except Exception as exc:
        print('%r generated an exception: %s' % (futures_response[future], exc))

  with open('read_messages.bin', 'wb') as read_messages_file:
      pickle.dump(read_messages, read_messages_file)

  print("done")
  with open("output_threaded.csv", "w", newline="") as output_csv_path:
    headers = ["Sender", "Count"]
    csv_writer = csv.DictWriter(output_csv_path, fieldnames=headers)
    csv_writer.writeheader()
    for sender, count in senders.items():
      csv_writer.writerow({"Sender": sender, "Count":count})

if __name__ == '__main__':
    main()