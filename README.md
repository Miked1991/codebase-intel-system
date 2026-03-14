*** 🗺️ Overview ***

The Brownfield Cartographer is a production-grade codebase intelligence system designed for Forward Deployed Engineers (FDEs) who need to rapidly understand unfamiliar codebases. It transforms "navigation blindness" into architectural clarity through automated static analysis, data lineage tracing, and LLM-powered semantic understanding.

*** 📦 Installation ***
** Prerequisites **

# Python 3.10 or higher

# Git

# Groq API key for LLM features


Quick Install
bash

# Clone the repository

git clone <https://github.com/yourusername/brownfield-cartographer.git>
cd brownfield-cartographer

# Create virtual environment (recommended)

python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies

pip install -e .


*** Using UV (Faster Installation) ***

# bash

# Install uv

pip install uv

# Install dependencies

uv pip install -e .


*** Environment Setup (Optional - for LLM Features) ***

# Create a .env file in the project root:
# bash

# Get your API key from <https://console.groq.com>

# GROQ_API_KEY=gsk_your_api_key_here

# Optional: Configure models

# FAST_MODEL=mixtral-8x7b-32768
# EXPENSIVE_MODEL=llama2-70b-4096



*** Example Workflows ***

1. Full Analysis of a New Codebase
bash

# python -m src.cli analyze ~/projects/ol-data-platform -it

This will:

    Run complete analysis (Surveyor + Hydrologist agents)

    Save artifacts to ~/projects/ol-data-platform/.cartography/

    Launch interactive shell for querying

***expected output***

🚀 Brownfield Cartographer - Codebase Intelligence System
════════════════════════════════════════════════════════════════

📂 Repository: /Users/user/projects/ol-data-platform
📁 Output: .cartography
🔄 Mode: Full

────────────────────────────────────────
PHASE 1: Surveyor Agent - Static Structure Analysis
────────────────────────────────────────
🔍 Surveyor: Analyzing repository...
  Found 342 SQL files, 156 Python files, 89 YAML files
✅ Surveyor: Analyzed 587 files

────────────────────────────────────────
PHASE 2: Hydrologist Agent - Data Lineage Analysis
────────────────────────────────────────
💧 Hydrologist: Analyzing data lineage...
  Found 863 datasets
  Found 496 transformations
✅ Hydrologist: Lineage graph built

────────────────────────────────────────
PHASE 4: Archivist Agent - Living Documentation
────────────────────────────────────────
📚 Archivist: Generating living documentation...
✅ Archivist: Artifacts saved to .cartography

════════════════════════════════════════════════════════════════
✅ ANALYSIS COMPLETE
════════════════════════════════════════════════════════════════

📊 Summary:
  • Modules analyzed: 0
  • Datasets found: 863
  • Transformations: 496
  • Analysis time: 47 seconds

📁 Artifacts saved to: .cartography/
  • module_graph.json - Module dependency graph
  • lineage_graph.json - Data lineage graph
  • cartography_trace.jsonl - Analysis audit log
