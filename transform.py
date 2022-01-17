import pandas as pd
import re
import nltk
from spellchecker import SpellChecker
from sqlalchemy import create_engine

class Transformtweets:

    def __init__(self, json_name):
        self.tweets = pd.read_json(json_name)
        self.clean_tweets = pd.DataFrame(self.tweets[['unique_source_id', 'date', 'source', 'author']].copy())

    #Remove HTML coded characters
    def remove_coded_char(self, text):
        return re.sub(r"&#\d\d(?:\d)?;","", text)

    #Find hashtags in tweet
    def find_hashtags(self, text):
        return re.findall(r"#(\w+)", text)

    #Find urls in tweet
    def find_url(self, text):
        return re.findall(r"((mailto\:|(news|(ht|f)tp(s?))\://){1}\S+)", text)[0][0]

    #Remove unnecessary text (urls, users, \n)
    def trunc_text(self, text):

        #Remove all text after url begins
        urlstart = re.search(r"((mailto\:|(news|(ht|f)tp(s?))\://){1}\S+)", text).start()
        sansurl = text[:urlstart]

        #Remove text after \n
        if re.search(r"\n", sansurl) == None:
            nstart = len(sansurl)
        else:
            nstart = re.search(r"\n", sansurl).start()
        sansn = sansurl[:nstart]

        #Remove author
        if re.search(r"- @(\w+)", sansn) == None:
            usern = len(sansurl)
        else:
            usern = re.search(r"- @(\w+)", sansn).start()
        sansuser = sansn[:usern]

        return sansuser

    #Tokenize text and remove punctuation
    def tokenizer(self, text):
        return nltk.tokenize.RegexpTokenizer(r"\w+").tokenize(text)

    #Spellcheck tokens, lowercase all and remove stopwords
    def spell_check_remove_stopwords(self, text):
        en_stopwords = nltk.corpus.stopwords.words("english")
        spellcheck = SpellChecker()
        result = []
        for token in text:
            token = spellcheck.correction(token).lower()
            if token not in en_stopwords:
                result.append(token)
        return result

    #Clean dataframe for SQL upload
    def produce_clean_dataframe(self):

        self.clean_tweets["kindaclean"] = self.tweets["raw_text"].apply(self.remove_coded_char)
        self.clean_tweets["hash_tags"] = self.clean_tweets["kindaclean"].apply(self.find_hashtags)
        self.clean_tweets["urls"] = self.clean_tweets["kindaclean"].apply(self.find_url)
        self.clean_tweets["kindaclean"] = self.clean_tweets["kindaclean"].apply(self.trunc_text)
        self.clean_tweets["tokens"] = self.clean_tweets["kindaclean"].apply(self.tokenizer)
        self.clean_tweets["tokens"] = self.clean_tweets["tokens"].apply(self.spell_check_remove_stopwords)
        self.clean_tweets.drop(columns="kindaclean", inplace=True)

    #Send shiny tweet tokens to DB
    def send_to_SQL(self):
        engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="12345",
                               db="tweetNLP"))

        self.clean_tweets.to_sql('clean', con = engine, if_exists = 'append', chunksize = 1000)
            

if __name__=="__main__":

    transformer = Transformtweets("scrape_data_for_challenge.json")
    transformer.produce_clean_dataframe()
    transformer.send_to_SQL()