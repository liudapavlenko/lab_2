import psycopg2
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from contextlib import contextmanager
from sqlalchemy import exists
import time

engine = create_engine('postgresql://postgres:qwerty@localhost:5432/Deanery')
Session = sessionmaker(bind=engine)

@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
Base = declarative_base()
class Catalog(Base):
    __tablename__ = 'Catalog'
    CatalogID = Column(Integer, primary_key=True)
    catalog_name = Column(String)


class Composer(Base):
    __tablename__ = 'Composer'
    ComposerID = Column(Integer, primary_key=True)
    Name = Column(String)
    Surname = Column(String)


class Notes(Base):
    __tablename__ = 'Notes'
    NotesID = Column(Integer, primary_key=True)
    ComposerID = Column(Integer, ForeignKey('Composer.ComposerID'))
    composition_name = Column(String)


class User(Base):
    __tablename__ = 'User'
    UserID = Column(Integer, primary_key=True)
    Name = Column(String)
    Surname = Column(String)


class Saving_notes(Base):
    __tablename__ = 'Saving_notes'
    Saving_notesID = Column(Integer, primary_key=True)
    CatalogID = Column(Integer, ForeignKey('Catalog.CatalogID'))
    NotesID = Column(Integer, ForeignKey('Notes.NotesID'))
    UserID = Column(Integer, ForeignKey('User.UserID'))


class Model:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='Deanery',
            user='postgres',
            password='qwerty',
            host='localhost',
            port=5432
        )

    def gener_add_user(self, num1, num2):
        c = self.conn.cursor()
        num = 0
        for i in range(num1, num2 + 1):
            c.execute('SELECT * FROM "User" WHERE "UserID" = %s', (i,))
            check = c.fetchall()
            if check:
                print("UserID %s already exists", i)
                num = 1

        if num == 0:
            # c.execute('INSERT INTO "User" ("UserID", "Name", "Surname") SELECT generate_series as UserID, chr(trunc(65+random()*25)::int) || chr(trunc(65+random()*25)::int) as Name,chr(trunc(65+random()*25)::int) || chr(trunc(65+random()*25)::int) as Surname FROM generate_series(%s, %s)', (num1, num2))
            c.execute('INSERT INTO "User" ("UserID", "Name", "Surname") SELECT generate_series as "UserID",to_tsvector(chr(trunc(65+random()*50)::int) || chr(trunc(65+random()*25)::int)|| chr(trunc(65+random()*25)::int)) as Name, chr(trunc(65+random()*25)::int) || chr(trunc(65+random()*25)::int) as Surname FROM generate_series(%s, %s)',(num1, num2))
            self.conn.commit()
        else:
            print("Error! Identifiers already exist")

    def request_1(self, val1, val2):
        c = self.conn.cursor()
        start_t = time.time()
        c.execute('SELECT * FROM "Composer" INNER JOIN "Notes" ON "Composer"."ComposerID" = "Notes"."ComposerID" WHERE "Composer"."Name" LIKE %s AND "NotesID" > %s group by "Composer"."ComposerID", "Notes"."NotesID";', (val1, val2))
        end_t = time.time()
        time_t = (end_t - start_t) * 1000
        print("Time 1:", time_t)
        return c.fetchall()


    def request_2(self, val1, val2):
        c = self.conn.cursor()
        start_t = time.time()
        c.execute('SELECT * FROM "Notes" INNER JOIN "Saving_notes" ON "Notes"."NotesID" = "Saving_notes"."NotesID" WHERE "Notes"."composition name" LIKE %s OR "Saving_notes"."CatalogID" > %s group by "Saving_notes"."Saving_notesID", "Notes"."NotesID"', (val1, val2))
        end_t = time.time()
        time_t = (end_t - start_t) * 1000
        print("Time 2:", time_t)
        return c.fetchall()

    def request_3(self, val1, val2):
        c = self.conn.cursor()
        start_t = time.time()
        c.execute('SELECT * FROM "Saving_notes" INNER JOIN "Notes" ON "Saving_notes"."NotesID" = "Notes"."NotesID" INNER JOIN "User" ON "Saving_notes"."UserID" = "User"."UserID" WHERE "User"."Name" LIKE %s AND "Saving_notes"."CatalogID" > %s group by "Saving_notes"."Saving_notesID", "Notes"."NotesID", "User"."UserID"', (val1, val2))
        end_t = time.time()
        time_t = (end_t - start_t) * 1000
        print("Time 3:", time_t)
        return c.fetchall()

    #for catalog
    def get_all_catalog(self):
        s = Session()
        return s.query(Catalog).all()

    def add_catalog(self, catalog_id, name):
        s = Session()
        check = s.query(exists().where(Catalog.CatalogID == catalog_id)).scalar()
        if (check):
            print("Error! This identifier already exists")
        else:
            catalog = Catalog(
                CatalogID=catalog_id,
                catalog_name=name
            )
            s.add(catalog)
            s.commit()
            print("User added successfully!")
        s.close()

    def update_catalog(self, catalog_id, catal_name):
        s = Session()
        catalog = s.query(Catalog).filter_by(CatalogID=catalog_id).first()
        if catalog:
            catalog.catalog_name = catal_name
            s.commit()
            print("Catalog updated successfully!")
        else:
            print("Error! Catalog does not exist")
        s.close()

    def delete_catalog(self, catalog_id):
        with session_scope() as s:
            saving_notes = s.query(Saving_notes).filter_by(CatalogID=catalog_id).all()
            if saving_notes:
                for notes in saving_notes:
                    s.delete(notes)
                s.commit()
                print("Catalog deleted successfully!")

            catalog = s.query(Catalog).get(catalog_id)
            if catalog:
                s.delete(catalog)
                s.commit()
                print("Catalog deleted successfully!")
            else:
                print("Error! Catalog does not exist")
    # for notes
    def add_notes(self, notes_id, composer_id, comp_name):
        s = Session()
        check = s.query(exists().where(Notes.NotesID == notes_id)).scalar()
        if (check):
            print("Error! This identifier already exists")
        else:
            check1 = s.query(exists().where(Composer.ComposerID == composer_id)).scalar()
            if (check1):

                notes = Notes(
                    NotesID=notes_id,
                    ComposerID=composer_id,
                    composition_name=comp_name
                )
                s.add(notes)
                s.commit()
                print("Notes added successfully!")
            else:
                print("Error! This identifier ComposerID already exists")
        s.close()

    def update_notes(self, notes_id, composer_id, comp_name):
        s = Session()
        notes = s.query(Notes).filter_by(NotesID=notes_id).first()
        composer = s.query(Composer).filter_by(ComposerID=composer_id).first()
        if notes and composer:
            notes.ComposerID = composer_id
            notes.composition_name = comp_name
            s.commit()
            print("Catalog updated successfully!")
        else:
            print("Error! Catalog does not exist")
        s.close()
    def delete_notes(self, notes_id):
        with session_scope() as s:
            saving_notes = s.query(Saving_notes).filter_by(NotesID=notes_id).all()
            if saving_notes:
                for save_note in saving_notes:
                    s.delete(save_note)
                s.commit()
                print("Catalog deleted successfully!")

            notes = s.query(Notes).get(notes_id)
            if notes:
                s.delete(notes)
                s.commit()
                print("Catalog deleted successfully!")
            else:
                print("Error! Catalog does not exist")

    def get_all_notes(self):
        s = Session()
        return s.query(Notes).all()
# for composer
    def add_composer(self, composer_id, name, surname):
        s = Session()
        check = s.query(exists().where(Composer.ComposerID == composer_id)).scalar()
        if (check):
            print("Error! This identifier already exists")
        else:
            composer = Composer(
                ComposerID=composer_id,
                Name=name,
                Surname=surname
            )
            s.add(composer)
            s.commit()
            print("Notes added successfully!")
        s.close()

    def update_composer(self, composer_id, name, surname):
        s = Session()
        composer = s.query(Composer).filter_by(ComposerID=composer_id).first()
        if composer:
            composer.Name = name
            composer.Surname = surname
            s.commit()
            print("Composer updated successfully!")
        else:
            print("Error! Composer does not exist")

        s.close()
    def delete_composer(self, composer_id):
        with session_scope() as s:
            notes_1 = [note.NotesID for note in s.query(Notes).filter_by(ComposerID=composer_id).all()]
            saving_notes = s.query(Saving_notes).filter(Saving_notes.NotesID.in_(notes_1)).all()
            composer = s.query(Composer).get(composer_id)
            notes = s.query(Notes).filter_by(ComposerID=composer_id).all()
            if composer and notes_1 and saving_notes:
                for note in saving_notes:
                    s.delete(note)
                s.commit()
                print("Notes deleted successfully from Saving notes!")

            if composer and notes:
                for note in notes:
                    s.delete(note)
                s.commit()
                print("Composer deleted successfully from Notes!")
            if composer:
                s.delete(composer)
                s.commit()
                print("Composer deleted successfully from Composer!")
            else:
                print(f"Error! Composer does not exist")

    def get_all_composer(self):
        s = Session()
        return s.query(Composer).all()

# for user
    def add_user(self, user_id, name, surname):
        s = Session()
        check = s.query(exists().where(User.UserID == user_id)).scalar()
        if (check):
            print("Error! This identifier already exists")
        else:
            user = User(
                UserID=user_id,
                Name=name,
                Surname=surname
            )
            s.add(user)
            s.commit()
            print("User added successfully!")
        s.close()
    def get_all_user(self):
        s = Session()
        return s.query(User).all()

    def update_user(self, user_id, name, surname):
        s = Session()
        user = s.query(User).filter_by(UserID=user_id).first()
        if user:
            user.Name = name
            user.Surname = surname
            s.commit()
            print("User updated successfully!")
        else:
            print("Error! User does not exist")

        s.close()
    def delete_user(self, user_id):
        with session_scope() as s:
            saving_notes = s.query(Saving_notes).filter_by(UserID=user_id).all()
            if saving_notes:
                for save_note in saving_notes:
                    s.delete(save_note)
                s.commit()
                print("Catalog deleted successfully!")

            user = s.query(Notes).get(user_id)
            if user:
                s.delete(user)
                s.commit()
                print("Catalog deleted successfully!")
            else:
                print("Error! Catalog does not exist")

#save notes
    def add_save_notes(self, save_notes_id, catalog_id, notes_id, user_id):
        s = Session()
        check = s.query(exists().where(Saving_notes.Saving_notesID == save_notes_id)).scalar()
        if (check):
            print("Error! This identifier already exists")
        else:
            check1 = s.query(exists().where(Catalog.CatalogID == catalog_id)).scalar()
            check2 = s.query(exists().where(Notes.NotesID == notes_id)).scalar()
            check3 = s.query(exists().where(User.UserID == user_id)).scalar()
            if check1 and check2 and check3:
                save_notes = Saving_notes(
                    Saving_notesID=save_notes_id,
                    CatalogID=catalog_id,
                    NotesID=notes_id,
                    UserID=user_id
                )
                s.add(save_notes)
                s.commit()
                print("Saving_notes added successfully!")
            else:
                print("Error! This identifier does not exist")
        s.close()
    def get_all_save_notes(self):
        s = Session()
        return s.query(Saving_notes).all()

    def update_save_notes(self, catalog_id, notes_id, user_id, save_notes_id):
        s = Session()
        saving_notes = s.query(Saving_notes).filter_by(Saving_notesID=save_notes_id).first()
        notes = s.query(Notes).filter_by(NotesID=notes_id).first()
        catalog = s.query(Catalog).filter_by(CatalogID=catalog_id).first()
        user = s.query(User).filter_by(UserID=user_id).first()
        if saving_notes and catalog and notes and user:
            saving_notes.CatalogID = catalog_id
            saving_notes.NotesID = notes_id
            saving_notes.UserID = user_id
            s.commit()
            print("Saving notes updated successfully!")
        else:
            print("Error! Saving notes does not exist")
        s.close()
    def delete_save_notes(self, save_notes_id):
        s = Session()
        save_notes = s.query(Saving_notes).get(save_notes_id)
        if save_notes:
            s.delete(save_notes)
            s.commit()
            print("Saving notes deleted successfully!")
        else:
            print("Error! Saving notes does not exist")
