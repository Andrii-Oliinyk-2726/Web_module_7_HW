from sqlalchemy import func, desc, select, and_

from src.models import Teacher, Student, Discipline, Grade, Group
from src.db import session


def select_one():
    """
    1. Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id: int):
    """
    2. Найти студента с наивысшим средним баллом по определенному предмету.
    :param discipline_id:
    :return:
    """
    r = session.query(Discipline.name,
                      Student.fullname,
                      func.round(func.avg(Grade.grade), 2).label('avg_grade')
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return r

"""
select s.id, s.fullname, g.grade, g.date_of
from grades g
  inner join students s on s.id = g.student_id
where g.discipline_id = 2
  and s.group_id = 2
  and g.date_of = (select max(date_of) -- находим последнее занятие для данной группы по данному предмету
                   from grades g2
                     inner join students s2 on s2.id = g2.student_id
                   where g2.discipline_id = g.discipline_id
                     and s2.group_id = s.group_id);

"""


def select_last(discipline_id, group_id):
    """
    3. Найти средний балл в группах по определенному предмету.
    :param discipline_id:
    :param group_id:
    :return:
    """
    subquery = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == discipline_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1).scalar_subquery())

    r = session.query(Discipline.name,
                      Student.fullname,
                      Group.name,
                      Grade.date_of,
                      Grade.grade
                      ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group)\
        .filter(and_(Discipline.id == discipline_id, Group.id == group_id, Grade.date_of == subquery)) \
        .order_by(desc(Grade.date_of)) \
        .all()
    return r


def select_avr_grade():
    """
    4. Найти средний балл на потоке (по всей таблице оценок).
    :return:
    """

    r = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).select_from(Grade).all()
    return r


def select_teacher_to_disciplines(teacher_id: int):
    """
    5. Найти какие курсы читает определенный преподаватель.
    :return:
    """

    r = session.query(Teacher.fullname, Discipline.name) \
        .select_from(Teacher) \
        .join(Discipline) \
        .filter(Teacher.id == teacher_id) \
        .all()
    return r


def select_students_in_group(group_id: int):
    """
    6. Найти список студентов в определенной группе.
    :return:
    """
    r = session.query(Group.name, Student.fullname) \
        .select_from(Student) \
        .join(Group) \
        .filter(Group.id == group_id) \
        .all()
    return r


def select_grades_in_group_in_discipline(group_id: int, discipline_id: int):
    """
    7. Найти оценки студентов в отдельной группе по определенному предмету.
    """

    r = session.query(Group.name, Discipline.name, Student.fullname, Grade.grade) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Discipline) \
        .filter(and_(Group.id == group_id, Discipline.id == discipline_id)) \
        .all()
    return r


def select_avr_grade_teacher(teacher_id: int):
    """
    8. Найти средний балл, который ставит определенный преподаватель по своим предметам.
    """

    r = session.query(Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.id) \
        .all()
    return r

def select_disciplines_by_student(student_id: int):
    """
    9. Найти список курсов, которые посещает определенный студент.
    """
    r = session.query(Student.fullname, Discipline.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .filter(Student.id == student_id) \
        .order_by(Discipline.name) \
        .all()
    return r

def select_10(student_id: int, teacher_id: int):
    """
    10. Список курсов, которые определенному студенту читает определенный преподаватель.
    """
    r = session.query(Student.fullname, Discipline.name) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(and_(Student.id == student_id), (Teacher.id == teacher_id)) \
        .order_by(Discipline.name) \
        .all()
    return r


if __name__ == '__main__':
    # print(select_one())
    # print(select_two(1))
    # print(select_last(1, 2))
    # print(select_avr_grade())
    # print(select_teacher_to_disciplines(1))
    # print(select_students_in_group(1))
    # print(select_grades_in_group_in_discipline(1, 1))
    # print(select_avr_grade_teacher(1))
    print(set(select_disciplines_by_student(1)))
    print(set(select_10(1, 1)))

