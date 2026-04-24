# Setting Up the `nldas` Environment

## Conda (Anaconda/Miniconda Environment)

### Create the Environment:
```bash
conda create --name nldas python=3.10  # Adjust Python version if needed
```

### Activate the Environment:
```bash
conda activate nldas
```

### Install Dependencies from `requirements.txt`:
```bash
pip install -r requirements.txt
```

---

## venv (Standard Python Virtual Environment)

### Create the Environment:
```bash
python -m venv nldas
```

### Activate the Environment:

#### macOS/Linux:
```bash
source nldas/bin/activate
```

#### Windows (Command Prompt):
```bash
nldas\Scripts\activate
```

#### Windows (PowerShell):
```powershell
.\nldas\Scripts\Activate
```

### Install Dependencies from `requirements.txt`:
```bash
pip install -r requirements.txt
```

