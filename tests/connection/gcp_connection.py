from google.auth import default

credentials, project = default()

def test_project_name():
    assert project == 'e-commerce-user-behavior-dev'

def test_exist_credentials():
    
    assert credentials is not None