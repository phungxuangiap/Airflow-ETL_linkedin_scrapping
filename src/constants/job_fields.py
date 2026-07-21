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

# Canonical role labels stored in Silver/Gold and exposed to dashboards.
# Keep genuinely different specializations separate. The order is the matching
# priority, so specific roles must precede the generic "software" fallback.
ROLE_LIST = [
    'technical lead', 'engineering manager', 'it manager',

    'fullstack', 'frontend', 'backend', 'mobile', 'game', 'embedded',
    'blockchain', 'salesforce',

    'data engineer', 'analytics engineer', 'data analyst', 'data science',
    'business intelligence', 'database', 'deep learning', 'machine learning',
    'computer vision', 'nlp', 'ai',

    'site reliability', 'devops', 'platform', 'solutions architect', 'cloud',
    'infrastructure', 'system administrator', 'network',

    'qa', 'security', 'it support',

    'business analyst', 'system analyst', 'product manager', 'product owner',
    'project manager', 'scrum master', 'designer',

    'sap consultant', 'erp consultant',

    'software'
]


# Recognition aliases. All aliases in one group are persisted as its canonical
# ROLE_LIST key, preventing synonymous titles from splitting dashboard metrics.
ROLE_ALIASES = {
    'technical lead': [
        'technical lead', 'tech lead', 'software lead', 'engineering lead'
    ],
    'engineering manager': ['engineering manager', 'software engineering manager'],
    'it manager': ['it manager', 'information technology manager'],

    'fullstack': [
        'fullstack developer', 'full stack developer', 'full-stack developer',
        'fullstack engineer', 'full stack engineer', 'full-stack engineer', 'fullstack'
    ],
    'frontend': [
        'frontend developer', 'front end developer', 'front-end developer',
        'frontend engineer', 'front end engineer', 'front-end engineer', 'frontend'
    ],
    'backend': [
        'backend developer', 'back end developer', 'back-end developer',
        'backend engineer', 'back end engineer', 'back-end engineer', 'backend'
    ],
    'mobile': [
        'android developer', 'android engineer', 'ios developer', 'ios engineer',
        'flutter developer', 'flutter engineer', 'react native developer',
        'react native engineer', 'mobile developer', 'mobile engineer', 'mobile'
    ],
    'game': ['game developer', 'game engineer', 'game programmer', 'game'],
    'embedded': [
        'embedded software engineer', 'embedded software developer',
        'embedded engineer', 'embedded developer', 'firmware engineer',
        'firmware developer', 'embedded'
    ],
    'blockchain': ['blockchain developer', 'blockchain engineer', 'blockchain'],
    'salesforce': ['salesforce developer', 'salesforce engineer', 'salesforce'],

    'data engineer': [
        'big data engineer', 'data engineer', 'etl engineer', 'etl developer'
    ],
    'analytics engineer': ['analytics engineer'],
    'data analyst': ['data analyst'],
    'data science': ['data scientist', 'data science'],
    'business intelligence': [
        'business intelligence developer', 'business intelligence analyst',
        'bi developer', 'bi analyst', 'business intelligence'
    ],
    'database': [
        'database administrator', 'database developer', 'database engineer',
        'database specialist', 'dba', 'database'
    ],
    'deep learning': ['deep learning engineer', 'deep learning'],
    'machine learning': [
        'machine learning engineer', 'machine learning developer', 'ml engineer',
        'ml developer', 'machine learning'
    ],
    'computer vision': [
        'computer vision engineer', 'computer vision developer', 'computer vision'
    ],
    'nlp': [
        'natural language processing engineer', 'nlp engineer', 'nlp developer', 'nlp'
    ],
    'ai': [
        'artificial intelligence engineer', 'ai engineer', 'ai developer',
        'artificial intelligence', 'ai'
    ],

    'site reliability': ['site reliability engineer', 'site reliability', 'sre'],
    'devops': ['devops engineer', 'devops developer', 'devops'],
    'platform': ['platform engineer', 'platform developer', 'platform'],
    'solutions architect': ['solutions architect', 'solution architect'],
    'cloud': ['cloud architect', 'cloud engineer', 'cloud developer', 'cloud'],
    'infrastructure': [
        'infrastructure engineer', 'infrastructure architect', 'infrastructure'
    ],
    'system administrator': [
        'linux administrator', 'system administrator', 'systems administrator', 'sysadmin'
    ],
    'network': [
        'network engineer', 'network administrator', 'network architect', 'network'
    ],

    'qa': [
        'quality assurance engineer', 'quality assurance analyst', 'qa engineer',
        'qa analyst', 'automation tester', 'manual tester', 'performance tester',
        'test automation engineer', 'test engineer', 'software tester', 'qa'
    ],
    'security': [
        'cybersecurity analyst', 'cyber security analyst', 'information security analyst',
        'penetration tester', 'security engineer', 'security analyst', 'soc analyst',
        'cybersecurity', 'cyber security', 'security'
    ],
    'it support': [
        'technical support engineer', 'helpdesk technician', 'help desk technician',
        'it support engineer', 'it support specialist', 'it support'
    ],

    'business analyst': ['business analyst'],
    'system analyst': ['systems analyst', 'system analyst'],
    'product manager': ['product manager'],
    'product owner': ['product owner'],
    'project manager': ['project manager'],
    'scrum master': ['scrum master'],
    'designer': [
        'ui ux designer', 'ui/ux designer', 'ux designer', 'ui designer',
        'product designer', 'web designer', 'designer'
    ],

    'sap consultant': ['sap consultant'],
    'erp consultant': ['erp consultant'],

    'software': [
        'software engineer', 'software developer', 'web developer', 'web engineer',
        'application developer', 'application engineer', 'software'
    ]
}


# Flat, priority-preserving list consumed by DuckDB's keyword matcher.
ROLE_KEYWORDS = [
    alias
    for canonical_role in ROLE_LIST
    for alias in ROLE_ALIASES[canonical_role]
]

# Role slugs used to generate crawler source URLs.
JOB_SOURCE_ROLE_SLUGS = [
    "backend",
    "frontend",
    "fullstack",
    # "web-developer",
    "mobile",
    # "android-developer",
    # "ios-developer",
    # "flutter-developer",
    # "react-native-developer",
    # "java-developer",
    "python",
    # "golang-developer",
    # "nodejs-developer",
    # "php-developer",
    # "dotnet-developer",
    # "ruby-developer",
    # "c++-developer",
    "embedded",
    # "firmware-engineer",
    "game",
    "blockchain",
    "AI",
    # "machine-learning-engineer",
    # "deep-learning-engineer",
    # "computer-vision-engineer",
    "data",
    # "business-intelligence-analyst",
    # "bi-developer",
    # "database",
    "devops",
    # "platform-engineer",
    "cloud",
    # "cloud-architect",
    # "solutions-architect",
    # "system-administrator",
    # "linux-administrator",
    # "network-engineer",
    "security",
    # "cybersecurity-analyst",
    # "information-security-analyst",
    # "penetration-tester",
    "qa",
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
    "it-support",
    # "helpdesk-technician",
    # "erp-consultant",
    # "salesforce-developer",
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
