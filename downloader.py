import pandas as pd
from tqdm import tqdm
import time 
import os

from bs4 import BeautifulSoup
import requests
import pickle

def readHTML(url):


    response = requests.get(url)
    html_content = response.content

    # Parse the HTML document with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract the plain text from the HTML document
    text = soup.get_text()
    return text


downloadRaw = True
project_folder = "C:/Users/debryu/Desktop/VS_CODE/HOME/ANLP/ANLP_Crawler"
save_folder = project_folder + "/datasets/raw_pdfs/"

# Directory where to get all the links
directory = project_folder + "/dataset/"
# get the list of all files and directories in the specified directory
files = os.listdir(directory)

len_files = len(files)

start_from = (0, 0)

global_id = 0
if(downloadRaw == True):
    for i,file in enumerate(files):
        if(i < start_from[0]):
            continue

        path = os.path.join(directory, file)
        print(f'Processing batch {i}/{len_files}...')
        # Import the dataframe
        # Load the CSV file into a DataFrame
        df = pd.read_csv(path)
        j = 0
        for index_id,link in enumerate(tqdm(df['PDF_link'], desc="PDFs")):
            j += 1
            if(j<start_from[1]):
                continue

            text = readHTML(link)

            
            #print('text',text)
            #print('link',link)
            #print('id',df['ID'][index_id])
            metadata = df['ID'][index_id].split(' ')
            doc_name = metadata[0]
            location = metadata[1]
            #print('dn',doc_name)
            with open(save_folder + f'document_{global_id}_{doc_name}.pickle', 'wb') as f:
                pickle.dump({'text': text, 'id': doc_name, 'location':location, 'link': link, 'counter': global_id}, f)
            
            global_id += 1
            
            

else:
    for i,file in enumerate(files):
        if(i < start_from[0]):
            continue

        path = os.path.join(directory, file)
        print(f'Processing batch {i}/{len_files}...')
        # Import the dataframe
        # Load the CSV file into a DataFrame
        df = pd.read_csv(path)
        j = 0
        for link in tqdm(df['PDF_link'], desc="PDFs"):
            j += 1
            if(j<start_from[1]):
                continue

            name = str(i) +'_'+str(j) + ".txt"
            text = readHTML(link)
            #text = text.replace('\udc00', '-unk-') # Replace surrogate with empty string
            text = text.encode('ascii', 'ignore').decode('ascii')
            with open(save_folder + name, 'w') as f:
                f.write(text)
            try:
                with open(save_folder + name, 'w') as f:
                    f.write(text[0:20820])
            except Exception as e:
                text = text.encode('ascii', 'ignore').decode('ascii')
                with open(save_folder + name, 'w') as f:
                    f.write(text[0:20820])
            
        print(df)