# CODEBASE.md - Living Architecture Context

*Generated: 2026-03-14 23:44:34*

## 🏗️ Architecture Overview

**Total Modules:** 1165
**Languages:** python (209), sql (610), yaml (311), unknown (35)
**Datasets:** 863
**Transformations:** 496

**Domain Distribution:**
- monitoring: 2 modules
- ingestion: 5 modules
- analytics: 2 modules
- transformation: 8 modules
- core_business: 3 modules
- critical_path: 1145 modules

## 🎯 Critical Path (Top Modules by PageRank)

- **dg_projects\canvas\canvas_tests\__init__.py**
- **dg_projects\canvas\canvas\assets\canvas.py**
- **dg_projects\canvas\canvas\assets\__init__.py**
- **dg_projects\canvas\canvas\defs\__init__.py**
- **dg_projects\canvas\canvas\lib\canvas.py**

## 💾 Data Sources & Sinks

**Source Datasets (Entry Points):**
- page_and_video
- problems_events
- page_views_table
- page_video_problems
- video_pre_query

**Sink Datasets (Exit Points):**
- open_learning
- afact_video_engagement
- dim_platform
- tfact_course_navigation_events
- tfact_studentmodule_problems

## ⚠️ Known Technical Debt

**Dead Code Candidates:**
- bin\dbt-create-staging-models.py
- bin\dbt-local-dev.py
- bin\uv-operations.py
- dg_deployments\reconcile_edxorg_partitions.py
- dg_projects\__init__.py

**Documentation Drift:**

## 📈 High-Velocity Files (Last 30 days)

- **.pre-commit-config.yaml**: 1 changes
- **build.yaml**: 1 changes
- **docker-compose.yaml**: 1 changes
- **renovate.json**: 1 changes
- **AGENTS.md**: 1 changes
- **README.md**: 1 changes

## 📋 Module Purpose Index

- **bin\dbt-create-staging-models.py**: This script automates the generation of dbt sources and staging models by interacting with a dbt project, discovering tables, and creating necessary YAML and SQL files. It provides a command-line interface to customize the generation process based on schema, prefix, and other parameters. The script can be used to streamline the development and maintenance of data pipelines in a dbt environment.
- **bin\dbt-local-dev.py**: This code is a unified CLI tool for local dbt development with DuckDB + Iceberg, providing commands for registering AWS Glue Iceberg tables as DuckDB views, testing Glue/Iceberg connectivity, and cleaning up Trino development schemas. The tool enables developers to streamline their local development workflow, ensuring seamless integration with AWS Glue and Trino.
- **bin\uv-operations.py**: This script automates the execution of uv commands across all code locations in the dg_projects directory, discovering and processing directories containing a pyproject.toml file. It provides options for verbose output and error handling, allowing users to customize the execution process. The script is designed to streamline the uv command workflow for developers working with multiple code locations.
- **dg_deployments\reconcile_edxorg_partitions.py**: This script reconciles and corrects invalid course IDs in edxorg archive asset partitions and S3 objects, ensuring consistency and accuracy in course data. It identifies and fixes course ID discrepancies in various asset types, including course structures, course XML, and database tables, by updating partition keys and S3 object paths. The script also deletes invalid dynamic partition keys, maintaining data integrity and organization.
- **dg_projects\__init__.py**: This code serves as the entry point for the dg_projects package, providing a centralized location for importing and initializing project-related functionality. It enables developers to easily access and utilize project-specific modules and classes. By defining this package, it facilitates a structured and organized approach to project development and maintenance.
- **bin\utils\chunk_tracking_logs_by_day.py**: This script is designed to reorganize tracking logs stored in Amazon S3 by chunking them by date, allowing for consistent path formatting across time boundaries. It takes a source bucket and a destination bucket as input, and processes files in the root of the source bucket to be located in path prefixes that are chunked by date. The script can be run in various modes, including dry run, destructive copy, and cleanup, to accommodate different use cases.
- **dg_projects\b2b_organization\__init__.py**: This module serves as a package initializer for the B2B Organization component, providing a centralized entry point for related functionality and dependencies. It enables the organization's business logic and data management to be accessed and utilized by other components within the system. This module facilitates the integration and reuse of B2B Organization-related code across the application.
- **dg_projects\student_risk_probability\__init__.py**: This code is part of a module that calculates and manages student risk probability, providing a framework for assessing and analyzing the likelihood of students' academic success or failure. It likely integrates with other systems to gather relevant data and generate insights for educational institutions or administrators. The module's primary function is to support data-driven decision making in student risk management.
- **dg_projects\b2b_organization\b2b_organization\definitions.py**: This code defines a data export pipeline for B2B organization data, which involves exporting data from a Vault storage system to an S3 bucket. The pipeline is designed to be environment-agnostic, with configuration options for different environments such as development, testing, and production.
- **dg_projects\b2b_organization\b2b_organization\__init__.py**: This module serves as the entry point for the B2B Organization application, providing a foundation for organizing and managing business-to-business relationships and interactions. It enables the creation and management of organizational structures, customer relationships, and associated business processes. By establishing a centralized framework, this module facilitates efficient and effective B2B operations.
- **dg_projects\b2b_organization\b2b_organization_tests\__init__.py**: This code serves as the entry point for a test suite, providing a centralized location for importing and organizing test modules within the B2B Organization project. It enables the execution of comprehensive tests to ensure the correctness and reliability of the B2B Organization functionality.
- **dg_projects\b2b_organization\b2b_organization\assets\data_export.py**: This code exports organization administration data from a database, filters the data by a specific organization key, and stores the filtered data in a CSV file on S3, along with metadata about the export, including the number of rows, file size, and timestamp. The exported data is versioned using a SHA-256 hash of the file contents.
- **dg_projects\b2b_organization\b2b_organization\assets\__init__.py**: This code is an empty initialization file for a Python package, likely used to define the package's namespace and provide a central location for importing sub-modules. It serves as a placeholder for future development and organization of the package's functionality.
- **dg_projects\b2b_organization\b2b_organization\defs\__init__.py**: This code defines the initialization and setup for the B2B Organization module, providing a foundation for its functionality and integration with other components. It establishes the necessary configurations and dependencies required for the module to operate effectively. This module is a critical component in the overall B2B system architecture.
- **dg_projects\b2b_organization\b2b_organization\partitions\b2b_organization.py**: This code defines a dynamic partitioning scheme for a data asset, allowing for the efficient processing and storage of large datasets related to B2B organizations. The partitioning scheme enables scalable data management and analysis. It facilitates the organization and querying of B2B organization data.
- **dg_projects\b2b_organization\b2b_organization\partitions\__init__.py**: This code defines the initialization and setup for the B2B Organization partitions, providing a foundation for data partitioning and organization within the B2B system. It establishes the necessary structures and relationships for efficient data management and scalability. This module serves as a critical component in maintaining data integrity and consistency across the B2B Organization.
- **dg_projects\b2b_organization\b2b_organization\sensors\b2b_organization.py**: This code defines a data sensor that periodically queries a database for a list of B2B organizational customers and triggers data exports for each organization, updating dynamic partitions as necessary. The sensor runs daily and is designed to ensure data freshness and consistency in a data warehouse.
- **dg_projects\b2b_organization\b2b_organization\sensors\__init__.py**: This code serves as the entry point for the B2B Organization sensors module, providing a centralized location for importing and initializing various sensor-related functionality. It enables the integration of sensor data into the B2B Organization system, facilitating data-driven decision-making and business insights.
- **dg_projects\canvas\canvas\definitions.py**: Here is a 2-3 sentence purpose statement explaining what this code does:

This code defines a data pipeline that exports Canvas course content and metadata for specified course IDs, utilizing a Google Sheet to determine the course IDs to process. The pipeline is scheduled to run every 6 hours, and it utilizes a Vault for authentication and a Google Service Account for interacting with the Google Sheet. The exported data is stored in an S3 bucket.
- **dg_projects\canvas\canvas\__init__.py**: This code serves as the entry point for the Canvas module, providing a foundation for interacting with the Canvas learning management system. It enables users to manage courses, assignments, and other educational content within the Canvas platform. By utilizing this module, developers can streamline their integration with Canvas and enhance their educational applications.

... and 1145 more modules