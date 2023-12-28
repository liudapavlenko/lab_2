from model import Model
from view import View

class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            category = self.show_menu()
            if category == '1':
                self.run_add()
            elif category == '2':
                self.run_view()
            elif category == '3':
                self.run_update()
            elif category == '4':
                self.run_delete()
            elif category == '5':
                self.gener_user()
            elif category == '6':
                self.requests()
            elif category == '7':
                break

    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add")
        self.view.show_message("2. View")
        self.view.show_message("3. Update")
        self.view.show_message("4. Delete")
        self.view.show_message("5. Generate new information in User")
        self.view.show_message("6. 3 requests")
        self.view.show_message("7. Quit")
        return input("Enter your choice: ")


    def menu(self):
        self.view.show_message("1. Catalog")
        self.view.show_message("2. Notes")
        self.view.show_message("3. Composer")
        self.view.show_message("4. User")
        self.view.show_message("5. Save notes")
        self.view.show_message("6. Quit")
        return input("Enter your choice: ")

    # run menu


    def run_view(self):
        print("\nView:")
        while True:
            category2 = self.menu()
            if category2 == '1':
                self.view_catalog_al()
            elif category2 == '2':
                self.view_notes_al()
            elif category2 == '3':
                self.view_composer_al()
            elif category2 == '4':
                self.view_user_al()
            elif category2 == '5':
                self.view_save_notes_al()
            elif category2 == '6':
                break

    def run_add(self):
        self.view.show_message("\nAdd:")
        while True:
            category1 = self.menu()
            if category1 == '1':
                self.add_catalog()
            elif category1 == '2':
                self.add_notes()
            elif category1 == '3':
                self.add_composer()
            elif category1 == '4':
                self.add_user()
            elif category1 == '5':
                self.add_save_notes()
            elif category1 == '6':
                break

    def run_update(self):
        self.view.show_message("\nUpdate:")
        while True:
            category3 = self.menu()
            if category3 == '1':
                self.update_catalog()
            elif category3 == '2':
                self.update_notes()
            elif category3 == '3':
                self.update_composer()
            elif category3 == '4':
                self.update_user()
            elif category3 == '5':
                self.update_save_notes()
            elif category3 == '6':
                break

    def run_delete(self):
        self.view.show_message("\nDelete:")
        while True:
            category4 = self.menu()
            if category4 == '1':
                self.delete_catalog()
            elif category4 == '2':
                self.delete_notes()
            elif category4 == '3':
                self.delete_composer()
            elif category4 == '4':
                self.delete_user()
            elif category4 == '5':
                self.delete_save_notes()
            elif category4 == '6':
                break

    def gener_user(self):
        num1, num2 = self.view.input_num()
        self.model.gener_add_user(num1, num2)
        print("Added successfully in User!")

    def requests(self):
        val1, val2 = self.view.input_req1()
        if val2.isdigit():
            request_1 = self.model.request_1(val1, val2)
            self.view.show_request_1(request_1)
        else:
            print("Error! you entered an incorrect type.")

        val3, val4 = self.view.input_req2()
        if val4.isdigit():
            request_2 = self.model.request_2(val3, val4)
            self.view.show_request_2(request_2)
        else:
            print("Error! you entered an incorrect type.")

        val5, val6 = self.view.input_req3()
        if val6.isdigit():
            request_3 = self.model.request_3(val5, val6)
            self.view.show_request_3(request_3)
        else:
            print("Error! you entered an incorrect type.")

    def add_catalog(self):
        catalog_name = self.view.get_catalog_input()
        catalog_id = self.view.get_catalogID()
        if catalog_id.isdigit():
            self.model.add_catalog(catalog_id, catalog_name)
        else:
            print("Error! you entered an incorrect type.")

    def update_catalog(self):
        catalog_id = self.view.get_catalogID()
        catalog_name = self.view.get_catalog_input()
        if catalog_id.isdigit():
            self.model.update_catalog(catalog_id, catalog_name)
        else:
            print("Error! you entered an incorrect type.")


    def view_catalog_al(self):
        catalog = self.model.get_all_catalog()
        self.view.show_catalog_alchemy(catalog)

    def delete_catalog(self):
        catalog_id = self.view.get_catalogID()
        if catalog_id.isdigit():
            self.model.delete_catalog(catalog_id)
        else:
            print("Error! you entered an incorrect type.")

    # notes
    def add_notes(self):
        composer_id, comp_name = self.view.get_notes_input()
        notes_id = self.view.get_notesID()
        if notes_id.isdigit() and composer_id.isdigit():
            self.model.add_notes(notes_id, composer_id, comp_name)
        else:
            print("Error! you entered an incorrect type.")

    def view_notes_al(self):
        notes = self.model.get_all_notes()
        self.view.show_notes_alchemy(notes)

    def update_notes(self):
        notes_id = self.view.get_notesID()
        composer_id, comp_name = self.view.get_notes_input()
        if notes_id.isdigit() and composer_id.isdigit():
            self.model.update_notes(notes_id, composer_id, comp_name)
        else:
            print("Error! you entered an incorrect type.")

    def delete_notes(self):
        notes_id = self.view.get_notesID()
        if notes_id.isdigit():
            self.model.delete_notes(notes_id)
        else:
            print("Error! you entered an incorrect type.")


# composer
    def add_composer(self):
        name, surname = self.view.get_composer_input()
        composer_id = self.view.get_composerID()
        if composer_id.isdigit() and name.isalpha() and surname.isalpha():
            self.model.add_composer(composer_id, name, surname)
        else:
            print("Error! you entered an incorrect type.")

    def view_composer_al(self):
        composers = self.model.get_all_composer()
        self.view.show_composer_alchemy(composers)

    def update_composer(self):
        composer_id = self.view.get_composerID()
        name, surname = self.view.get_composer_input()
        if composer_id.isdigit() and name.isalpha() and surname.isalpha():
            self.model.update_composer(composer_id, name, surname)
        else:
            print("Error! you entered an incorrect type.")


    def delete_composer(self):
        composer_id = self.view.get_composerID()
        if composer_id.isdigit():
            self.model.delete_composer(composer_id)
        else:
            print("Error! you entered an incorrect type.")

    # user
    def add_user(self):
        name, surname = self.view.get_user_input()
        user_id = self.view.get_userID()
        if user_id.isdigit():
            self.model.add_user(user_id, name, surname)
        else:
            print("Error! you entered an incorrect type.")

    def view_user_al(self):
        users = self.model.get_all_user()
        self.view.show_user_alchemy(users)

    def update_user(self):
        user_id = self.view.get_userID()
        name, surname = self.view.get_user_input()
        if user_id.isdigit() and name.isalpha() and surname.isalpha():
            self.model.update_user(user_id, name, surname)
            self.view.show_message("User updated successfully!")
        else:
            print("Error! you entered an incorrect type.")

    def delete_user(self):
        user_id = self.view.get_userID()
        if user_id.isdigit():
            self.model.delete_user(user_id)
        else:
            print("Error! you entered an incorrect type.")

    #save notes
    def add_save_notes(self):
        save_notes_id = self.view.get_save_notesID()
        catalog_id, notes_id, user_id = self.view.get_save_notes_input()
        if save_notes_id.isdigit() and catalog_id.isdigit() and notes_id.isdigit() and user_id.isdigit():
            self.model.add_save_notes(save_notes_id, catalog_id, notes_id, user_id)
        else:
            print("Error! you entered an incorrect id.")

    def view_save_notes_al(self):
        save_notes = self.model.get_all_save_notes()
        self.view.show_save_notes_alchemy(save_notes)

    def update_save_notes(self):
        save_notes_id = self.view.get_save_notesID()
        catalog_id, notes_id, user_id = self.view.get_save_notes_input()
        if save_notes_id.isdigit() and catalog_id.isdigit() and notes_id.isdigit() and user_id.isdigit():
            self.model.update_save_notes(catalog_id, notes_id, user_id, save_notes_id)
        else:
            print("Error! you entered an incorrect id.")

    def delete_save_notes(self):
        save_notes_id = self.view.get_save_notesID()
        if save_notes_id.isdigit():
            self.model.delete_save_notes(save_notes_id)
        else:
            print("Error! you entered an incorrect id.")
