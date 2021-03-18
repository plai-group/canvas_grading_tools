import os
from canvasapi import Canvas
import janitor
from pprint import pprint
from joblib.parallel import Parallel, delayed
import requests
import re
import pandas as pd

STUDENT_CSV = '/Users/vmasrani/dev/phd/teaching/553/grading/students.csv'

def pmap(f, arr, n_jobs=-1, prefer='threads', verbose=10):
    return Parallel(n_jobs=n_jobs, prefer=prefer, verbose=verbose)(delayed(f)(i) for i in arr)


def get_grade_map_for_question(question_id: str, rubric: pd.DataFrame):
    grade_map =  (pd
     .DataFrame(rubric['ratings'].loc[question_id])
     .set_index('description')['points']
    ).to_dict()

    # let zeros and A+ pass through
    grade_map[0] = 0
    grade_map["A+"] = rubric.loc[question_id, 'points']

    return grade_map

def get_perfect(submissions: list, rubric: pd.DataFrame):
    full_points = {i:"A+" for i in rubric.index}
    return pd.DataFrame({s.user_id:full_points for s in submissions}).T


def get_user_sheet(course):
    students = pd.read_csv(STUDENT_CSV)
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
        # if score < total_autograde:
        #     print(sub.user_id)
        return (sub.user_id, score)
    auto_grade = pmap(_autograde, submissions, n_jobs=128) # runs in parallel
    df = pd.DataFrame(auto_grade).set_index(0)
    df.columns = [autograde_rubric_id]
    return df.astype(str)

def get_course_assignment(course: int, assignment: int):
    API_URL = "https://canvas.ubc.ca/" # default is canvas.ubc
    API_KEY = os.getenv("CANVAS_API")  # canvas.ubc instructor token
    canvas = Canvas(API_URL, API_KEY)
    course = canvas.get_course(course)
    assignment = course.get_assignment(assignment)

    rubric = (pd.DataFrame(assignment.rubric).set_index('id'))
    all_submissions = list(assignment.get_submissions())
    valid_submissions = [sub for sub in all_submissions if sub.submission_type is not None]
    invalid_submission = [sub for sub in all_submissions if sub.submission_type is None]
    return course, assignment, rubric, valid_submissions, invalid_submission


def get_pre_grades(course, rubric, valid_submissions, autograde=False):
    scores = get_perfect(valid_submissions, rubric)
    if autograde:
        autograde_rubric_id = rubric[rubric.description == "Autograded Exercises"].index.tolist()
        scores[autograde_rubric_id] = autograde(valid_submissions, autograde_rubric_id)
    students = get_user_sheet(course)
    comments = scores.copy().applymap(lambda x: '')
    return scores, students, comments

def upload(submissions: list,
           scores: pd.DataFrame,
           comments: pd.DataFrame,
           rubric: pd.DataFrame):

    def _get_rubric_assessment(sid: int):
        return {qid:{"points":points.loc[sid, qid],
                     "comments":comments.loc[sid, qid]} for qid in scores.columns}

    def _letters_to_points(question: pd.Series):

        grade_map = get_grade_map_for_question(question.name, rubric)
        return question.astype(str).replace(grade_map).astype(float)

    points = scores.apply(_letters_to_points)

    for sub in submissions:
        print(f"Uploading {sub.user_id}: ")
        print("---------------------------")
        rubric = _get_rubric_assessment(sub.user_id)
        sub.edit(rubric_assessment=rubric)
        pprint(rubric)
        print("===========================")


if __name__ == "__main__":
    course, assignment, rubric, valid_submissions, invalid_submission = get_course_assignment(59085, 826521)
    scores, students, comments = get_pre_grades(course, rubric, valid_submissions)

    # uploads all A+, scores can be overwritten with student-specific grades
    upload(valid_submissions, scores, comments, rubric)

