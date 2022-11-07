import json

import dotenv
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(100), unique=True)

    def __repr__(self):
        return f'Publisher #{self.id}: {self.name}'


class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(100), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)

    publisher = relationship(Publisher, backref="books")

    def __repr__(self):
        return f'Book #{self.id}: {self.title} by Publisher #{self.id_publisher}'


class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(100), unique=True)

    def __repr__(self):
        return f'Shop #{self.id}: {self.name}'


class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship(Book, backref='stocks')
    shop = relationship(Shop, backref='stocks')

    def __repr__(self):
        return f'Stock #{self.id}: Book #{self.id_book} from Shop #{self.id_shop}'


class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.DECIMAL, nullable=False)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship(Stock, backref='sales')

    def __repr__(self):
        return f'Sale #{self.id}: {self.count} pieces of Stock #{self.id_stock} at a price of {self.price} on {self.date_sale}'


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    ENV_PATH = '.env'
    DSN = dotenv.get_key(ENV_PATH, 'DSN')

    engine = sq.create_engine(DSN)
    create_tables(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    with open('fixtures/test_data.json', mode='r') as file:
        data = json.load(file)

        for row in filter(lambda x: x['model'] == 'publisher', data):
            element = Publisher(id=row['pk'],
                                name=row['fields']['name'])
            session.add(element)
        session.commit()
        for row in filter(lambda x: x['model'] == 'shop', data):
            element = Shop(id=row['pk'],
                           name=row['fields']['name'])
            session.add(element)
        session.commit()
        for row in filter(lambda x: x['model'] == 'book', data):
            element = Book(id=row['pk'],
                           title=row['fields']['title'],
                           id_publisher=row['fields']['id_publisher'])
            session.add(element)
        session.commit()
        for row in filter(lambda x: x['model'] == 'stock', data):
            element = Stock(id=row['pk'],
                            id_book=row['fields']['id_book'],
                            id_shop=row['fields']['id_shop'],
                            count=row['fields']['count'])
            session.add(element)
        session.commit()
        for row in filter(lambda x: x['model'] == 'sale', data):
            element = Sale(id=row['pk'],
                           price=row['fields']['price'],
                           id_stock=row['fields']['id_stock'],
                           count=row['fields']['count'],
                           date_sale=row['fields']['date_sale'])
            session.add(element)
        session.commit()

    query_publisher = input()
    query_publisher_id = None
    if query_publisher.isdigit():
        query_publisher_id = int(query_publisher)

    if query_publisher_id is None:
        print(f'Shops selling books published by {query_publisher}:')
        q = session.query(Shop.name).join(Stock, Stock.id_shop == Shop.id) \
            .join(Book, Book.id == Stock.id_book) \
            .join(Publisher, Publisher.id == Book.id_publisher) \
            .filter(Publisher.name == query_publisher) \
            .all()
    else:
        print(f'Shops selling books published by Publisher #{query_publisher_id}:')
        q = session.query(Shop.name).join(Stock, Stock.id_shop == Shop.id) \
            .join(Book, Book.id == Stock.id_book) \
            .filter(Book.id_publisher == query_publisher_id) \
            .all()
    if q:
        print(*set([record[0] for record in q]), sep=',\n')
    else:
        print('not found')

    session.close()
