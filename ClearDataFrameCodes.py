import re
import string
import nltk
from langdetect import detect

#reger for emojies
emojies_pattern = re.compile("["
    u"\U0001F600-\U0001F64F"  # emoticons
    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
    u"\U0001F680-\U0001F6FF"  # transport & map symbols
    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
    u"\U00002500-\U00002BEF"  # chinese char
    u"\U00002702-\U000027B0"
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001f926-\U0001f937"
    u"\U00010000-\U0010ffff"
    u"\u2640-\u2642"
    u"\u2600-\u2B55"
    u"\u200d"
    u"\u23cf"
    u"\u23e9"
    u"\u231a"
    u"\ufe0f"  # dingbats
    u"\u3030"
                    "]+", re.UNICODE)

nltk.download('stopwords')
STOPWORDS = nltk.corpus.stopwords.words('turkish')

def CleanLinkandEmojies(cleaning_variables):
    try:
        #clear emoies in text
        for i, key in enumerate(cleaning_variables):
            cleaning_variables[i] = emojies_pattern.sub(r'', key)

    except:
        print("Error while cleaning emojies")

    #clear links from text
    try:
        for i, key in enumerate(cleaning_variables):
            cleaning_variables[i] = re.sub('http://\S+|https://\S+', '', key)
    except:
        print("Error while cleaning links")

    return(tuple(cleaning_variables))

def ClearPunctuations(cleaning_variables):
    try:
        for i, key in enumerate(cleaning_variables):
            cleaning_variables[i] = [words for words in key if words not in string.punctuation]
            cleaning_variables[i] =''.join(cleaning_variables[i])
    except:
        print("Error while in Punctuations")

    return(tuple(cleaning_variables))

def RemoveStopwords(cleaning_stopwords):
    try:
        cleaning_stopwords = cleaning_stopwords.lower()
        tweet_words = cleaning_stopwords.split()
        cleaned_text = [word for word in tweet_words if not word  in STOPWORDS]
        cleaning_stopwords = ' '.join(cleaned_text)

    except:
        print("Error in stop words")

    return (cleaning_stopwords)


def CheckTweetLanguage(language, text):
    try:
        if(len(text) > 2) and type(text) == str:
            return True if detect(text) == language else False

    except Exception as e:
        print('Error while checking tweet language')
        print(e)
        return False

