class View:
    def show_request_1(self, requests):
        if requests:
            print("Request 1:")
            for request in requests:
                print(f"ComposerID: {request[0]}, Name: {request[1]}, Surname: {request[2]}, NotesID: {request[3]}, ComposerID: {request[4]}, composition name: {request[5]}")
        else:
            print("No request found.")

    def show_request_2(self, requests):
        if requests:
            print("Request 2:")
            for request in requests:
                print(f"NotesID: {request[0]}, ComposerID: {request[1]}, composition name: {request[2]}, save_notesID: {request[3]}, catalog_id: {request[4]}, notes_id: {request[5]}, user_id: {request[6]}")
        else:
            print("No request found.")

    def show_request_3(self, requests):
        if requests:
            print("Request 3:")
            for request in requests:
                print(f"Saving_notesID: {request[0]}, CatalogID: {request[1]}, NotesID: {request[2]}, UserID: {request[3]}, NotesID: {request[4]}, ComposerID: {request[5]}, composition name: {request[6]}, user_id: {request[7]}, name: {request[8]}, surname: {request[9]}")
        else:
            print("No request found.")

    def input_req1(self):
        val1 = input("Enter the first letter of the composer's name: ")
        val3 = val1 + "%"
        val2 = input("Enter number: ")
        return val3, val2

    def input_req2(self):
        val1 = input("Enter the first letter of the composition: ")
        val3 = val1 + "%"
        val2 = input("Enter number: ")
        return val3, val2

    def input_req3(self):
        val1 = input("Enter the first letter of the username: ")
        val3 = val1 + "%"
        val2 = input("Enter number: ")
        return val3, val2

    def input_num(self):
        num1 = int(input("Enter number 1: "))
        num2 = int(input("Enter number 2: "))
        return num1, num2

    # catalog alchemy
    def show_catalog_alchemy(self, catalogs):
        if catalogs:
            print("catalogs:")
            for catalog in catalogs:
                print(f"CatalogID: {catalog.CatalogID}, catalog name: {catalog.catalog_name}")
        else:
            print("No catalog found.")

    def show_catalog(self, catalogs):
        if catalogs:
            print("catalogs:")
            for catalog in catalogs:
                print(f"CatalogID: {catalog[0]}, catalog name: {catalog[1]}")
        else:
            print("No catalog found.")

    def get_catalog_input(self):
        catal_name = input("Enter catalog name: ")
        return catal_name

    def get_catalogID(self):
        catalogID = input("Enter catalog ID: ")
        return catalogID
    def show_composer_alchemy(self, composers):
        if composers:
            print("Users:")
            for composer in composers:
                print(f"ID: {composer.ComposerID}, Name: {composer.Name}, Surname: {composer.Surname}")
        else:
            print("No composer found.")

    def show_composer(self, composers):
        if composers:
            print("Users:")
            for composer in composers:
                print(f"ID: {composer[0]}, Name: {composer[1]}, Surname: {composer[2]}")
        else:
            print("No composer found.")

    def get_composer_input(self):
        Name = input("Enter composer Name: ")
        Surname = input("Enter composer Surname: ")
        return Name, Surname

    def get_composerID(self):
        composerID = input("Enter composer ID: ")
        return composerID

    # notes
    def show_notes_alchemy(self, notes_1):
        if notes_1:
            print("Notes:")
            for notes in notes_1:
                print(f"NotesID: {notes.NotesID}, ComposerID: {notes.ComposerID}, composition name: {notes.composition_name}")
        else:
            print("No notes found.")

    def show_notes_(self, notes_1):
        if notes_1:
            print("Notes:")
            for notes in notes_1:
                print(f"NotesID: {notes[0]}, ComposerID: {notes[1]}, composition name: {notes[2]}")
        else:
            print("No notes found.")

    def get_notes_input(self):
        composer_id = input("Enter composer ID: ")
        comp_name = input("Enter composition name: ")
        return composer_id, comp_name

    def get_notesID(self):
        notesID = input("Enter notes ID: ")
        return notesID

    def show_user_alchemy(self, users):
        if users:
            print("Users:")
            for User in users:
                print(f"ID: {User.UserID}, Name: {User.Name}, Surname: {User.Surname}")
        else:
            print("No users found.")

    def show_user(self, users):
        if users:
            print("Users:")
            for User in users:
                print(f"ID: {User[0]}, Name: {User[1]}, Surname: {User[2]}")
        else:
            print("No users found.")

    def get_user_input(self):
        Name = input("Enter user Name: ")
        Surname = input("Enter user Surname: ")
        return Name, Surname

    def get_userID(self):
        userID = input("Enter user ID: ")
        return userID

        # save notes
    def show_save_notes_alchemy(self, save_notes):
        if save_notes:
            print("Notes:")
            for notes in save_notes:
                print(
                    f"Saving_notesID: {notes.Saving_notesID}, CatalogID: {notes.CatalogID}, NotesID: {notes.NotesID}, UserID: {notes.UserID}")
        else:
            print("No save notes found.")

    def show_save_notes_(self, save_notes):
        if save_notes:
            print("Notes:")
            for notes in save_notes:
                print(
                    f"Saving_notesID: {notes[0]}, CatalogID: {notes[1]}, NotesID: {notes[2]}, UserID: {notes[3]}")
        else:
            print("No save notes found.")

    def get_save_notes_input(self):
        catalog_id = input("Enter catalog ID: ")
        notes_id = input("Enter notes ID: ")
        user_id = input("Enter user ID: ")
        return catalog_id, notes_id, user_id

    def get_save_notesID(self):
        save_notesID = input("Enter saving notes ID: ")
        return save_notesID


    # output
    def show_message(self, message):
        print(message)