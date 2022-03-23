import argparse
import os
import pandas as pd
import random
from lorem_text import lorem
import numpy as np

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input",
                        help="Path of all unprocessed CSV files generated from 'XMLToCSV.py'",
                        type=str)
    parser.add_argument("-o", "--output",
                        help="Target path for processed CSV files",
                        type=str)
    parser.add_argument("-f", "--fraction",
                        help="Fraction of article and inproceeding data to be sampled.",
                        type=int, default=0.01)
    return parser

def read_and_sample(input_path, random_state = 1, fraction = 0.01):
    list_of_files = os.listdir(input_path)
    if fraction*5 > 1:
                    fraction2 = fraction
    else:
        fraction2 = fraction*5
    for file in list_of_files:
        file_path = os.path.join(input_path, file)
        if 'header' in file:
                column_names = (pd.read_csv(file_path, sep = ';', header=0).columns)
                data_file = file_path.replace('_header', '')
                data = pd.read_csv(data_file, sep = ';', header=None, names = column_names, low_memory=False)
                if 'article' in file:
                        print('Reading Article Data.')
                        data = data.sample(frac = fraction, random_state = random_state)
                        article_data = data
                        article_data = article_data[[c for c in article_data if article_data[c].isna().sum() < 0.95*article_data.shape[0]]]
                elif 'inproceedings' in file:
                        print('Reading Inproceeding Data.')
                        data = data.sample(frac = fraction, random_state = random_state)
                        inproceedings_data = data
                        inproceedings_data = inproceedings_data[[c for c in inproceedings_data if inproceedings_data[c].isna().sum() < 0.95*inproceedings_data.shape[0]]]
                elif 'proceedings' in file:
                        print('Reading Proceeding Data.')
                        data = data.sample(frac = fraction2, random_state = random_state)
                        proceedings_data = data
                        proceedings_data = proceedings_data[['proceedings:ID', 'booktitle:string','ee:string[]', 'isbn:string[]'
                                                ,'mdate:date', 'publisher:string[]', 'series:string[]', 'volume:string']]
        elif file == 'dblp_author.csv':
                print('Reading Author Data.')
                author_data = pd.read_csv(file_path, sep = ';', header=0, low_memory=False)

    return(author_data, article_data, proceedings_data, inproceedings_data)

def create_keywords(df):
        key_words = []
        db_key_words = ['data management', 'indexing', 'data modeling', 'big data', 'data processing', 'data storage', 'data querying']
        it = 0
        for key, value in df['title:string'].iteritems():
                it += 1
                try:
                        possible_keywords = value.split()

                        sep = '|'
                        if it == 5:
                                key_words.append(sep.join(random.sample(db_key_words, 3)))
                                it = 0
                        elif len(possible_keywords) <= 3:
                                key_words.append(sep.join(possible_keywords))
                        else:
                                key_words.append(sep.join(random.sample(possible_keywords, 3)))
                except:
                        key_words.append(value)
        df["keywords"] = key_words
        return df

def create_abstract(df):
        abstracts = []
        for row in range(df.shape[0]):
                length = random.randint(3,10)
                abstracts.append(lorem.words(length))
        df["abstract:string"] = abstracts
        return df


def split_list_of_authors(df_column):
    authors = []
    for x in list(df_column):
        try:
            authors.extend(x.split('|')) 
        except:
            pass
    return authors

def create_citations(article_df, proceeding_df):
        article_ids = list(article_df['article:ID'])
        proceed_paper_ids = list(proceeding_df['inproceedings:ID'])
        citations_for_art = []
        citations_for_bk = []
        sep = '|'
        for key, value in article_df['article:ID'].iteritems():
                citation_list = []
                amount = random.randint(0,35)
                for citation in range(amount):
                        book_or_article = random.randint(0,1)
                        if book_or_article == 0:
                                cit = random.choice(article_ids)
                                if cit != value:
                                        citation_list.append(str(cit))
                                else:
                                        pass
                        else:
                                cit = random.choice(proceed_paper_ids)
                                if cit != value:
                                        citation_list.append(str(cit))
                                else:
                                        pass
                citations_for_art.append(sep.join(citation_list))
        for key, value in proceeding_df['inproceedings:ID'].iteritems():
                citation_list = []
                amount = random.randint(0,15)
                for citation in range(amount):
                        book_or_article = random.randint(0,1)
                        if book_or_article == 0:
                                cit = random.choice(article_ids)
                                if cit != value:
                                        citation_list.append(str(cit))
                                else:
                                        pass
                        else:
                                cit = random.choice(proceed_paper_ids)
                                if cit != value:
                                        citation_list.append(str(cit))
                                else:
                                        pass
                citations_for_bk.append(sep.join(citation_list))
        
        article_df['citations:string[]'] = citations_for_art
        proceeding_df['citations:string[]'] = citations_for_bk
        return(article_df, proceeding_df)

def choose_corresponding_author(df):
        corresponding_list = []
        co_author_list = []
        for key, value in df['author:string[]'].iteritems():
                try:
                        authors = value.split('|')
                        corr = random.choice(authors)
                        corresponding_list.append(corr)
                        authors.remove(corr)
                        co_authors = authors
                        co_authors = '|'.join(co_authors)
                        co_author_list.append(co_authors)

                except:
                        corresponding_list.append(value)
                        co_author_list.append("")

        df['corresponding'] = corresponding_list
        df['co_authors'] = co_author_list
        return df

def create_reviewers(article_df, proceeding_df):
        author = split_list_of_authors(article_df['author:string[]']) + split_list_of_authors(proceeding_df['author:string[]'])
        article_reviewers = []
        proceeding_paper_reviewer = []
        sep = '|'
        for key, value in article_df['author:string[]'].iteritems():
                try:
                        authors = value.split('|')
                except:
                        authors = ['']
                review_list = []
                amount = random.randint(1,4)
                for rev in range(amount):
                        reviewer = random.choice(author)
                        if reviewer not in authors:
                                review_list.append(reviewer)
                        else:
                                pass
                article_reviewers.append(sep.join(review_list))
        for key, value in proceeding_df['author:string[]'].iteritems():
                try:
                        authors = value.split('|')
                except:
                        authors = ['']
                review_list = []
                amount = random.randint(1,4)
                for rev in range(amount):
                        reviewer = random.choice(author)
                        if reviewer not in authors:
                                review_list.append(reviewer)
                        else:
                                pass
                proceeding_paper_reviewer.append(sep.join(review_list))
        
        article_df['reviewed_by:string[]'] = article_reviewers
        proceeding_df['reviewed_by:string[]'] = proceeding_paper_reviewer
        return(article_df, proceeding_df)

def join_proceeding_data(proceeding_df, inproceeding_df):
        res = inproceeding_df.merge(proceeding_df, how = 'inner', on='booktitle:string')
        columns = res.columns
        new_columns = []
        for col in columns:
                if '_x' in col:
                        new_columns.append('inproc' + col)
                elif '_y' in col:
                        new_columns.append('proc' + col)
                else:
                        new_columns.append(col)
        res.columns = new_columns
        return res

def create_location_dict(proceeding_df):
        cities = ['Antwerp', 'Brussels', 'Twente', 'Utrecht', 'Oosterbeek', 'Schaarbeek', 'The Hague', 'Barcelona', 'Kviv', 'Marioepol', 'Odessa'
        ,'Charkov', 'Stockholm', 'Te Anua', 'Aarschot', 'Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch']
        proc_cit = {}
        location_list = []
        for inproceedings in np.unique(proceeding_df['procmdate']):
                proc_cit[inproceedings] = random.choice(cities)
        for key, value in proceeding_df['procmdate'].iteritems():
                location_list.append(proc_cit[value])
        proceeding_df['location'] = location_list
        return proceeding_df

def join_author_data(author_df, df):
        author_df = author_df.rename(columns={'author:string':'corresponding', ':ID' : 'correspondingID'})
        res = df.merge(author_df, how='inner', on='corresponding')
        return res


def remove_format_spec(df):
        new_cols = []
        for col in df.columns:
                new_cols.append(col.split(':')[0])
        df.columns = new_cols
        return df

def preprocess_pipeline(input_path, output_path, fraction):
    random_state = 1
    print("------------------------------------------------")
    print("Looking for unprocessed files:")
    author_data, article_data, proceedings_data, inproceedings_data = read_and_sample(input_path = input_path, random_state=random_state, fraction = fraction)
    print("------------------------------------------------")
    print("unprocessed data read into data frames.")
    article_data = create_keywords(article_data)
    inproceedings_data = create_keywords(inproceedings_data)
    article_data = create_abstract(article_data)
    inproceedings_data = create_abstract(inproceedings_data)
    print("------------------------------------------------")
    print("Keywords & Abstracts Extracted.")
    article_data, inproceedings_data = create_citations(article_data, inproceedings_data)
    article_data, inproceedings_data = create_reviewers(article_data, inproceedings_data)
    print("------------------------------------------------")
    print("Citations & Reviews written.")
    article_data = choose_corresponding_author(article_data)
    inproceedings_data = choose_corresponding_author(inproceedings_data)
    article_data = join_author_data(author_data, article_data)
    paper_proceedings_data = join_proceeding_data(proceedings_data, inproceedings_data)
    print("------------------------------------------------")
    print("Data frames joined together.")
    article_data = remove_format_spec(article_data)
    paper_proceedings_data = remove_format_spec(paper_proceedings_data)
    paper_proceedings_data = create_location_dict(paper_proceedings_data)
    print("------------------------------------------------")
    print("Writing to output:")
    article_data.to_csv(os.path.join(output_path, 'article.csv'), sep = ';', index=None)
    paper_proceedings_data.to_csv(os.path.join(output_path, 'proceeding_papers.csv'), sep = ';', index=None)
    print("------------------------------------------------")
    print("Preprocessing Completed.")

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    preprocess_pipeline(args.input, args.output, args.fraction)
