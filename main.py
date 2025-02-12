import pandas as pd
import datetime


def data_creation():
# Sample data
    data = {
        "ISBN": ["978-0131101630", "978-0201616224", "978-0132350884", "978-0596007126", "978-0137081073", "978-0321125217",
                 "978-0131872486", "978-0131103627", "978-0201433071", "978-0596009205", "978-0134734722", "978-0133542824",
                 "978-0131573043", "978-0131101630", "978-0201616224"],
        "Book_Title": ["The C Programming Language", "Design Patterns", "Clean Code", "Head First Java", "Effective Java",
                       "Refactoring", "The Pragmatic Programmer", "The Art of Computer Programming",
                       "Introduction to Algorithms", "Python Cookbook", "Design Patterns Explained", "Clean Code",
                       "Modern Operating Systems", "The C Programming Language", "Design Patterns"],
        "Book_Author": ["Brian W. Kernighan, Dennis M. Ritchie", "Erich Gamma, Richard Helm, Ralph Johnson, John Vlissides",
                        "Robert C. Martin", "Kathy Sierra, Bert Bates", "Joshua Bloch", "Martin Fowler", "Andrew Hunt, David Thomas",
                        "Donald E. Knuth", "Thomas H. Cormen, Charles E. Leiserson", "David Beazley, Brian K. Jones", "Erich Gamma,"
                        " Richard Helm, Ralph Johnson, John Vlissides", "Robert C. Martin", "Andrew S. Tanenbaum", "Brian W. Kernighan,"
                        " Dennis M. Ritchie", "Erich Gamma, Richard Helm, Ralph Johnson, John Vlissides"],
        "Year_Of_Publication": [1978, 1994, 2008, 2003, 2008, 1999, 1999, 1968, 2009, 2013, 2002, 2008, 2008, 1978, 1994],
        "Publisher": ["Prentice Hall", "Addison-Wesley", "Pearson", "O'Reilly Media", "Addison-Wesley", "Addison-Wesley",
                      "Addison-Wesley", "Addison-Wesley", "MIT Press", "O'Reilly Media", "Addison-Wesley", "Pearson", "Prentice Hall",
                      "Prentice Hall", "Addison-Wesley"],
        "Image_URL_S": ["https://example.com/small1.jpg", "https://example.com/small2.jpg",
                        "https://example.com/small3.jpg", "https://example.com/small4.jpg",
                        "https://example.com/small5.jpg", "https://example.com/small6.jpg",
                        "https://example.com/small7.jpg", "https://example.com/small8.jpg",
                        "https://example.com/small9.jpg", "https://example.com/small10.jpg",
                        "https://example.com/small11.jpg", "https://example.com/small12.jpg",
                        "https://example.com/small13.jpg", "https://example.com/small14.jpg",
                        "https://example.com/small15.jpg"],
        "Image_URL_M": ["https://example.com/medium1.jpg", "https://example.com/medium2.jpg",
                        "https://example.com/medium3.jpg", "https://example.com/medium4.jpg",
                        "https://example.com/medium5.jpg", "https://example.com/medium6.jpg",
                        "https://example.com/medium7.jpg", "https://example.com/medium8.jpg",
                        "https://example.com/medium9.jpg", "https://example.com/medium10.jpg",
                        "https://example.com/medium11.jpg", "https://example.com/medium12.jpg",
                        "https://example.com/medium13.jpg", "https://example.com/medium14.jpg",
                        "https://example.com/medium15.jpg"],
        "Image_URL_L": ["https://example.com/large1.jpg", "https://example.com/large2.jpg",
                        "https://example.com/large3.jpg", "https://example.com/large4.jpg",
                        "https://example.com/large5.jpg", "https://example.com/large6.jpg",
                        "https://example.com/large7.jpg", "https://example.com/large8.jpg",
                        "https://example.com/large9.jpg", "https://example.com/large10.jpg",
                        "https://example.com/large11.jpg", "https://example.com/large12.jpg",
                        "https://example.com/large13.jpg", "https://example.com/large14.jpg",
                        "https://example.com/large15.jpg"]
    }

    df = pd.DataFrame(data)
    return df



def data_cleaning(**kwargs):

    task_instance = kwargs['ti']
    df = kwargs['data']

    new_df = df.dropna()
    df_unique = new_df.drop_duplicates(subset='Book_Title', keep='first')

    clean_df = df_unique.drop(columns=['Image_URL_S', 'Image_URL_M', 'Image_URL_L'])

    task_instance.xcom_push(key='book_list', value=clean_df.to_json())


def create_author_books_df_with_count(df):
    # Group by 'Author' and aggregate the 'Title' into a list of books
    author_books_df = df.groupby('Book_Author')['Book_Title'].apply(list).reset_index()

    # Add a new column 'Book Count' to store the count of books for each author
    author_books_df['Book_Count'] = author_books_df['Book_Title'].apply(len)

    author_books_df.xcom_push(key='author_list', value=author_books_df)


   # return author_books_df


def calculate_book_age(df):
    # Get the current year
    current_year = datetime.datetime.now().year
    # Calculate the age of the book
    df['Book_Age'] = current_year - df['Year-Of-Publication']
    return df

def year_conversion(df):
    df['Year-Of-Publication'] = pd.to_numeric(df['Year-Of-Publication'], errors='coerce')
    return df


#
#df = data_creation()
#final_df = data_cleaning(df)
#print(final_df)
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

