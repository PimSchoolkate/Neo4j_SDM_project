import os
import pandas as pd
import random
from lorem_text import lorem
import math
import argparse


def get_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-i", "--input",
                   help="Location of where the article.csv and proceeding_papers.csv are stored",
                   type=str)
    p.add_argument("-o", "--output",
                   help="location of where the author.csv should be stored",
                   type=str)
    return p


def get_authors(pr_df, ar_df):
    new = "|".join(map(str, list(ar_df['author'].unique()) + list(pr_df['author'].unique())))
    return list(set(new.split('|')))


def assign_aff(df):
    organisations = ['The Useless Duck Company', 'UPC', 'Pies As Nodes Inc.', 'Free University Brussels',
                     'Twente University', 'Zelensky Inc.', 'Vueling', 'Lenovo', 'Acer', 'Gadjah Mada University']
    org = organisations * math.floor(df.size / len(organisations))
    org = org + organisations[:df.size % len(organisations)]
    return org


def assign_rev_dec(df):
    dec = []
    rev = []
    for i in range(0, df.shape[0]):
        if random.randint(0, 100) < 20:
            dec.append('no')
        else:
            dec.append('yes')

        text_length = random.randint(3, 20)
        rev.append(lorem.words(text_length))
    return rev, dec


def create_a3_data(processed_data_path, update_data_path):
    article_data = pd.read_csv(os.path.join(processed_data_path, 'article.csv'), sep=';')
    proceeding_papers = pd.read_csv(os.path.join(processed_data_path, 'proceeding_papers.csv'), sep=';')

    authors = pd.DataFrame()

    authors['name'] = get_authors(article_data, proceeding_papers)
    authors['affiliation'] = assign_aff(authors)

    authors['review'], authors['decision'] = assign_rev_dec(authors)

    authors.to_csv(os.path.join(update_data_path, 'authors.csv'), sep=';', index=False)


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    create_a3_data(args.input, args.output)
    print("=================================================")
    print("Created authors.csv at path: " + str(args.output))
    print("=================================================")
