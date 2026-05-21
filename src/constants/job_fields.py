"""
Job field constants and mappings
"""

# Job fields for Silver layer
JOB_FIELDS = [
    'title',
    'company_id',
    'source_name',
    'location_type',
    'employment_type',
    'date_posted',
    'techstacks',
    'job_url',
    'salary',
    'level',
    'role',
    'number_applicants',
    'processed_at'
]

# Company fields for Silver layer
COMPANY_FIELDS = [
    'id',
    'name',
    'industry',
    'company_size',
    'location',
    'source_name'
]

# Technology stack list (from system_consts.py)
TECH_LIST = [
    # Programming Languages
    'python', 'java', 'c++', 'c#', 'javascript', 'typescript', 'go', 'rust', 'kotlin', 'swift',
    'php', 'ruby', 'r', 'scala', 'matlab', 'groovy', 'perl',

    # Web Development
    'html', 'css', 'react', 'angular', 'vue', 'svelte', 'nodejs', 'express', 'django', 'flask',
    'fastapi', 'spring boot', 'asp.net', 'rails', 'laravel', 'webpack', 'babel',

    # Data & Analytics
    'sql', 'spark', 'hadoop', 'kafka', 'elasticsearch', 'pandas', 'numpy', 'scikit-learn',
    'dbt', 'airflow', 'prefect', 'snowflake', 'bigquery', 'redshift', 'hive', 'presto',

    # AI/Machine Learning
    'tensorflow', 'pytorch', 'keras', 'scikit-learn', 'nlp', 'computer vision', 'deep learning',
    'llm', 'bert', 'transformers', 'hugging face', 'xgboost', 'lightgbm',

    # DevOps & Infrastructure
    'docker', 'kubernetes', 'jenkins', 'gitlab-ci', 'github actions', 'circleci', 'terraform',
    'ansible', 'prometheus', 'grafana', 'elk stack', 'datadog', 'newrelic', 'linux',

    # Cloud Platforms
    'aws', 'azure', 'gcp', 'ec2', 's3', 'lambda', 'rds', 'dynamodb', 'cloudformation',
    'aks', 'app service', 'cosmos db', 'gce', 'cloud storage', 'bigquery', 'cloud dataflow',

    # Database & Storage
    'postgresql', 'mysql', 'mongodb', 'redis', 'cassandra', 'dynamodb', 'neo4j', 'firebase',
    'memcached', 'duckdb', 'influxdb', 'timescaledb',

    # Mobile Development
    'swift', 'kotlin', 'react native', 'flutter', 'dart', 'objective-c', 'android',
    'ios', 'xamarin', 'ionic', 'expo',

    # DevTools & Version Control
    'git', 'github', 'gitlab', 'bitbucket', 'svn', 'jira', 'trello', 'notion',
    'vscode', 'intellij', 'visual studio', 'postman', 'insomnia',

    # Testing & Quality
    'junit', 'pytest', 'mocha', 'jest', 'selenium', 'cypress', 'postman', 'jmeter',
    'sonarqube', 'coverage', 'tdd', 'bdd',

    # Message Queues & Streaming
    'rabbitmq', 'kafka', 'activemq', 'aws sqs', 'pubsub', 'nats', 'zeromq', 'pulsar',
]

# Role list
ROLE_LIST = [
    'software', 'data', 'devops', 'qa', 'security', 'mobile', 'game', 'embedded', 'cloud',
    'fullstack', 'frontend', 'backend', 'ai', 'ml', 'nlp', 'computer vision', 'analytics',
    'business intelligence', 'database', 'infrastructure', 'network', 'support',
    'business analyst', 'product manager', 'project manager', 'designer', 'product owner'
]

# Employment types
EMPLOYMENT_TYPES = [
    'FULL_TIME',
    'PART_TIME',
    'CONTRACT',
    'TEMPORARY',
    'INTERNSHIP',
    'VOLUNTEER'
]

# Location types
LOCATION_TYPES = [
    'REMOTE',
    'HYBRID',
    'ON_SITE',
    'N/A'
]

# Seniority levels
SENIORITY_LEVELS = [
    'Entry',
    'Junior',
    'Mid-Level',
    'Senior',
    'Lead',
    'Principal',
    'Staff',
    'Manager',
    'Director',
    'VP',
    'C-Level'
]
