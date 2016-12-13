db.examMarks.drop();
db.createCollection('examMarks');
db.examMarks.insert({
    student_id: 1,
    name: 'Mary',
    surname: 'Murray',
    nationality: 'Irish',
    age: 45,
    courses:[
        {
            course_id: 'DB',
            course_name: 'Databases',
            credits: 10,
            mark: 56,
            examdata: '10/10/2011'
        },
        {
            course_id: 'MA',
            course_name: 'Maths',
            credits: 5,
            mark: 76,
            examdata: '09/11/2012'
        },
        {
            course_id: 'PR',
            course_name: 'Programming',
            credits: 15,
            mark: 45,
            examdata: '02/07/2014'
        }
    ]
});
db.examMarks.insert({
    student_id: 2,
    name: 'Bill',
    surname: 'Bellichick',
    nationality: 'American',
    age: 32,
    courses:[
        {
            course_id: 'DB',
            course_name: 'Databases',
            credits: 10,
            mark: 55,
            examdata: '10/10/2011'
        },
        {
            course_id: 'MA',
            course_name: 'Maths',
            credits: 5,
            mark: 87,
            examdata: '09/11/2012'
        },
        {
            course_id: 'PR',
            course_name: 'Programming',
            credits: 15,
            mark: 45,
            examdata: '02/07/2014'
        }
    ]
});
db.examMarks.insert({
    student_id: 3,
    name: 'Tom',
    surname: 'Brady',
    nationality: 'Canadian',
    age: 22,
    courses:[
        {
            course_id: 'DB',
            course_name: 'Databases',
            credits: 10,
            mark: 34,
            examdata: '10/10/2011'
        },
        {
            course_id: 'MA',
            course_name: 'Maths',
            credits: 5,
            mark: 56,
            examdata: '09/11/2012'
        }
    ]
});
db.examMarks.insert({
    student_id: 4,
    name: 'John',
    surname: 'Bale',
    nationality: 'English',
    age: 24,
    courses:[
        {
            course_id: 'DB',
            course_name: 'Databases',
            credits: 10,
            mark: 71,
            examdata: '10/10/2011'
        },
        {
            course_id: 'MA',
            course_name: 'Maths',
            credits: 5,
            mark: 88,
            examdata: '09/11/2012'
        },
        {
            course_id: 'PR',
            course_name: 'Programming',
            credits: 15,
            mark: 95,
            examdata: '02/07/2014'
        }
    ]
});

db.failures.drop();
db.examMarks.aggregate([ { $unwind: '$courses'}, 
                         { $match: { 'courses.mark': { "$lt" : 40 } } }, 
                         { $out: "failures" } ]);
db.failures.find({});

db.passed.drop();
db.examMarks.aggregate( [ {$unwind: "$courses"}, 
                          {$match: { 'courses.mark': { "$gte" : 40 } }}, 
                          {$group: { _id: '$courses.course_name' , total: { $sum :1}}}, 
                          {$out : 'passed'}] );
db.passed.find({});

db.average.drop();
db.examMarks.aggregate( [ { $project : {averages : {'$avg' : '$courses.mark'}}}, 
                          { $sort: { averages:-1} },
                          { $limit:1},  
                          { $out: "average" } ] );
db.average.find({});

