"""Main application"""
import os
import sys
from time import sleep

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from app.logger import logger
from app.save import save_df_to_csv
from app.view import forklift_logistics_dataset, view_data
# from app.check import check_data



class MenuOption(Enum):
    """Menu options enumeration"""
    SAVE = 1
    VIEW = 2
    CHECK = 3
    EXIT = 4

class DataFrameType(Enum):
    """Available dataframe types"""
    ALL = "all"
    EMR = "emr"
    OPERATIONS = "operations"
    NETLIST = "netlist"
    BIN_DISPATCH = "bin_dispatch"
    SHORE_HANDLING = "shore_handling"
    STUFFING = "stuffing"
    TRANSPORT = "transport"
    MISCELLANEOUS = "miscellaneous"

@dataclass
class AppConfig:
    """Application configuration"""
    version: str = "0.0.3"
    title: str = "Attica Invoice"
    author: str = "gmounac<at>outlook<dot>com"
    year: str = "2024"

class App:
    """main application"""

    def __init__(self) -> None:
        self.config = AppConfig()

    def clear_screen(self) -> None:
        """clears the screen based on the OS"""
        logger.info("Clearing screen")
        os.system("cls" if os.name == "nt" else "clear")

    def exit_application(self) -> None:
        """Gracefully exits the application"""
        logger.info("Exiting application")
        sleep(1)
        sys.exit(0)

    @property
    def greeting(self) -> str:
        """Returns formatted welcome message"""
        return f"""
            {self.config.title} v.{self.config.version}
            ---------------------------
            {self.config.author} (c) {self.config.year}
            
            Select an option:
            ----- 1. Save dataframe to CSV file
            ----- 2. View dataframe
            ----- 3. Check Logistics Record
            ----- 4. Exit Application
            """


    def get_dataframe_selection(self) -> Optional[str]:
        """Prompts user for dataframe selection with validation"""
        options_text = "\n".join(
            f"            {df.value} : {self._get_df_description(df)}"
            for df in DataFrameType
        )

        print(options_text)

        while True:
            choice = input("Select the dataframe or 'all' for all dataframes: ").lower()
            try:
                return DataFrameType(choice).value
            except ValueError:
                logger.warning("Invalid dataframe selection: %s", choice)
                print("Invalid selection. Please try again.")
                continue

    def _get_df_description(self, df_type: DataFrameType) -> str:
        """Returns description for each dataframe type"""
        descriptions = {
            DataFrameType.ALL: "For all of the dataframes",
            DataFrameType.EMR: "For shifting, PTI and Washing",
            DataFrameType.OPERATIONS: "Operations data",
            DataFrameType.NETLIST: "Genesis data sets",
            DataFrameType.BIN_DISPATCH: "IOT Scow transfer data",
            DataFrameType.SHORE_HANDLING: "Salt and Bin Tipping data",
            DataFrameType.STUFFING: "Plugging including stuffing of containers",
            DataFrameType.TRANSPORT:"Haulage,Shore Crane and Forklift data",
            DataFrameType.MISCELLANEOUS:"CCCS and Cross stuffing data"
        }
        return descriptions.get(df_type, "")

    def handle_save(self) -> None:
        """Handles the save operation with proper validation"""

        self.clear_screen()
        while True:
            choice = input("Continue Saving the file [Y/n] ").lower()
            if choice in ('y', 'yes'):
                data = self.get_dataframe_selection()
                if data:
                    logger.info("Initiating save operation for %s", data)
                    save_df_to_csv(data)
                menu = input("Return to the main menu [Y/n]").lower()
                if menu in ('y', 'yes'):
                    self.run()
                else:
                    self.exit_application()
            elif choice in ('n', 'no'):
                return
            else:
                print("Invalid choice. Please enter Y or N.")


    def run(self) -> None:
        """Main application loop with improved error handling"""
        logger.info("Starting application")

        while True:
            try:
                self.clear_screen()
                print(self.greeting)

                selection = input("Choose the option: ").strip()

                try:
                    option = MenuOption(int(selection))
                except ValueError:
                    print("Please enter a number between 1 and 4")
                    sleep(1)
                    continue

                match option:
                    case MenuOption.SAVE:
                        logger.info("Selected: Save files")
                        self.handle_save()
                    case MenuOption.VIEW:
                        logger.info("Selected: View dataframe")
                        self.clear_screen()
                     
                        view_data(forklift_logistics_dataset())

                        # Implement view functionality
                    case MenuOption.CHECK:
                        logger.info("Selected: Check logistics records")
                        self.clear_screen()
                        print("Checking logistics records...")
                    case MenuOption.EXIT:
                        self.exit_application()
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                self.exit_application()
            except (ValueError, IOError) as e:
                logger.error("Error processing input or file operation: %s", str(e))
                print(f"An error occurred: {str(e)}")
                sleep(2)
