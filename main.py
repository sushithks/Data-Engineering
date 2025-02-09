import pandas as pd
import datetime


def data_cleaning(ti,df):
    new_df = df.dropna()
    df_unique = new_df.drop_duplicates(subset='Book-Title', keep='first')
    df_unique.drop(columns=['Image-URL-S', 'Image-URL-M', 'Image-URL-L'], inplace=True)

    ti.xcom_push(key='book_list', value=df_unique)



def create_author_books_df_with_count(df):
    # Group by 'Author' and aggregate the 'Title' into a list of books
    author_books_df = df.groupby('Book-Author')['Book-Title'].apply(list).reset_index()

    # Add a new column 'Book Count' to store the count of books for each author
    author_books_df['Book Count'] = author_books_df['Book-Title'].apply(len)

    return author_books_df


def calculate_book_age(df):
    # Get the current year
    current_year = datetime.datetime.now().year
    # Calculate the age of the book
    df['Book_Age'] = current_year - df['Year-Of-Publication']
    return df

def year_conversion(df):
    df['Year-Of-Publication'] = pd.to_numeric(df['Year-Of-Publication'], errors='coerce')
    return df

#df = pd.read_csv('C:/Users/sushi/OneDrive/Desktop/Sushith/DE/books_data/books.csv', delimiter=';', on_bad_lines='skip')
#new_df= data_cleaning(df)
#print(new_df.columns)

#df = pd.read_csv('books.scv', delimiter=';', on_bad_lines='skip')

#print(df.info())
#year_conversion(df)
#print(df.info())
#new_df = create_author_books_df_with_count(df)
#new_df = data_cleaning(df)
#print(new_df.info())

