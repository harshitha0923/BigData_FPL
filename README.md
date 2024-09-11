
# Big Data FPL

## Project Overview
This project is focused on analyzing and processing Fantasy Premier League (FPL) data using Python. The project includes scripts to handle data streaming and data processing tasks, helping to extract insights and statistics from the FPL dataset.

## Contents

- `stream.py`: Handles data streaming, retrieves FPL data, and manages continuous updates.
- `master.py`: The main script for processing the streamed data and performing the necessary computations and analysis.

## Setup Instructions

### Prerequisites
Before running the project, make sure you have the following installed:
- Python 3.x
- Required libraries (you can install them using `pip`):
  ```bash
  pip install -r requirements.txt
  ```

### Running the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/harshitha0923/BigData_FPL.git
   ```
   
2. Navigate to the project directory:
   ```bash
   cd BigData_FPL-main
   ```

3. Run the streaming script to start pulling in data:
   ```bash
   python stream.py
   ```

4. Run the master script to process the streamed data:
   ```bash
   python master.py
   ```

## Contributing
Feel free to fork this repository, make improvements, and create pull requests. Contributions are welcome!

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
