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
    'python', 'java', 'c\\+\\+', 'c#', 'javascript', 'typescript', 'go', 'rust', 'kotlin', 'swift',
    'php', 'ruby', 'scala', 'matlab', 'groovy', 'perl',

    # Web Development
    'html', 'css', 'react', 'angular', 'vue', 'svelte', 'nodejs', 'express', 'django', 'flask',
    'fastapi', 'spring boot', 'asp\\.net', 'rails', 'laravel', 'webpack', 'babel',

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
    'software engineer', 'software developer', 'web developer', 'fullstack developer',
    'frontend developer', 'backend developer', 'software', 'fullstack', 'frontend', 'backend',

    'android developer', 'ios developer', 'flutter developer', 'react native developer',
    'mobile developer', 'mobile',

    'game developer', 'embedded software engineer', 'firmware engineer', 'game', 'embedded',

    'big data engineer', 'data engineer', 'data analyst', 'data scientist',
    'analytics engineer', 'business intelligence', 'bi developer', 'etl developer',
    'database administrator', 'database developer', 'machine learning engineer',
    'deep learning engineer', 'computer vision engineer', 'nlp engineer', 'ai engineer',
    'data', 'analytics', 'database', 'computer vision', 'nlp', 'ai', 'ml',

    'site reliability engineer', 'devops engineer', 'platform engineer', 'cloud architect',
    'cloud engineer', 'solutions architect', 'infrastructure engineer', 'system administrator',
    'linux administrator', 'network engineer', 'devops', 'sre', 'cloud', 'infrastructure', 'network',

    'quality assurance engineer', 'automation tester', 'manual tester', 'performance tester',
    'test engineer', 'qa engineer', 'qa',

    'cybersecurity analyst', 'information security analyst', 'penetration tester',
    'security engineer', 'soc analyst', 'security',

    'technical support engineer', 'helpdesk technician', 'it support', 'support',
    'business analyst', 'system analyst', 'product manager', 'product owner', 'project manager',
    'scrum master', 'ui ux designer', 'designer',

    'technical lead', 'engineering manager', 'it manager', 'erp consultant', 'sap consultant',
    'salesforce developer', 'blockchain developer'
]

# Role slugs used to generate crawler source URLs.
JOB_SOURCE_ROLE_SLUGS = [
    "software-engineer",
    # "backend-engineer",
    # "frontend-engineer",
    # "fullstack-engineer",
    # "web-developer",
    # "mobile-developer",
    # "android-developer",
    # "ios-developer",
    # "flutter-developer",
    # "react-native-developer",
    # "java-developer",
    # "python-developer",
    # "golang-developer",
    # "nodejs-developer",
    # "php-developer",
    # "dotnet-developer",
    # "ruby-developer",
    # "c++-developer",
    # "embedded-software-engineer",
    # "firmware-engineer",
    # "game-developer",
    # "blockchain-developer",
    # "ai-engineer",
    # "machine-learning-engineer",
    # "deep-learning-engineer",
    # "computer-vision-engineer",
    # "nlp-engineer",
    # "data-engineer",
    # "big-data-engineer",
    # "data-analyst",
    # "business-intelligence-analyst",
    # "bi-developer",
    # "data-scientist",
    # "database",
    # "etl-developer",
    # "analytics-engineer",
    # "devops-engineer",
    # "site-reliability-engineer",
    # "platform-engineer",
    # "cloud-engineer",
    # "cloud-architect",
    # "solutions-architect",
    # "system-administrator",
    # "linux-administrator",
    # "network-engineer",
    # "security-engineer",
    # "cybersecurity-analyst",
    # "information-security-analyst",
    # "penetration-tester",
    # "soc-analyst",
    # "qa-engineer",
    # "quality-assurance-engineer",
    # "performance-tester",
    # "business-analyst",
    # "system-analyst",
    # "product-owner",
    # "product-manager",
    # "project-manager",
    # "scrum-master",
    # "technical-lead",
    # "engineering-manager",
    # "it-manager",
    # "it-support",
    # "helpdesk-technician",
    # "technical-support-engineer",
    # "erp-consultant",
    # "sap-consultant",
    # "salesforce-developer",
    # "ui-ux-designer",
]

# Employment types
EMPLOYMENT_TYPES = [
    'FULL_TIME',
    'PART_TIME',
    'INTERNSHIP'
]

# Location types
LOCATION_TYPES = [
    'REMOTE',
    'HYBRID',
    'ON_SITE'
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
