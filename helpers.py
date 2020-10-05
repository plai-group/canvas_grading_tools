import janitor
import requests
import re
from joblib import Parallel, delayed
import pandas as pd
from ml_helpers import pmap

def get_grade_map_for_question(question_id: str, rubric: pd.DataFrame):
    return (pd
     .DataFrame(rubric['ratings'].loc[question_id])
     .set_index('description')['points']
    ).to_dict()

def get_perfect(submissions: list, rubric: pd.DataFrame):
    full_points = {r['id']:"A+" for r in rubric}
    return pd.DataFrame({s.user_id:full_points for s in submissions}).T


def get_user_sheet(course):
    students = pd.read_csv('https://raw.github.ubc.ca/MDS-2020-21/GitHubLMS/master/data/students2020.csv?token=AAAALVOS3RY7VVM327V7YHC7P72WI')
    users = course.get_users()
    df = (pd
          .DataFrame([(user.sis_user_id, user.id) for user in users], columns=['student_number', "canvas_number"])
          .dropna()
          .change_type('student_number', int)
          .merge(students, on='student_number')[['student_number','canvas_number','cwl']]
         )
    return df


def autograde(submissions: list, autograde_rubric_id: str):
    def _autograde(sub):
        try:
            with requests.session() as s:
                html = s.get(sub.attachments[0]['url']).text
                autogrades = []
                start = []
                for match in re.finditer("rubric={autograde:(\d).*}", html):  # find all "autograde" rubrics
                    autogrades.append(int(re.findall("(\d+)", match.group())[0]))
                    start.append(match.start())
                start.append(-1)  # -1 represents end of file
                total_autograde = sum(autogrades)
                score = 0
                for i in range(len(start) - 1):
                    score += (autogrades[i] if "PASSED TESTS" in html[start[i] : start[i + 1]] else 0)
        except:
            score = 0
        if score < total_autograde:
            print(sub.user_id)
        return (sub.user_id, score)
    auto_grade = pmap(_autograde, submissions, n_jobs=128) # runs in parallel
    df = pd.DataFrame(auto_grade).set_index(0)
    df.columns = [autograde_rubric_id]
    return df.astype(str)

def letters_to_points(scores: pd.DataFrame, rubric: pd.DataFrame):
    def _letters_to_points(question: pd.Series):
        grade_map = get_grade_map_for_question(question.name, rubric)
        return question.replace(grade_map)

    return scores.apply(_letters_to_points)

def get_rubric_assessment(student_id: int, points: pd.DataFrame):
    return {k:{"points":str(v)} for k, v in points.loc[student_id].to_dict().items()}

