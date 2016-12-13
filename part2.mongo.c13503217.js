db.createCollection('c13503217_teams');

db.c13503217_teams.insert({
	team_id: "eng1",
	date_founded: new Date("Oct 04, 1896"),
     league: "Premier League",
	 points: 62,
	 name: "Manchester United",
     players: [ { p_id: "Rooney", goal: 85, caps: 125, age: 28 },
              { p_id: "Scholes", goal: 15, caps: 225, age: 28 },
			  { p_id: "Giggs", goal: 45, caps: 359, age: 38 } ]
	 });

db.c13503217_teams.insert({
	team_id: "eng2",
	date_founded: new Date("Oct 04, 1899"),
     league: "Premier League",
	 points: 52,
	 name: "Arsenal",
     players: [ { p_id: "Mata", goal: 5, caps: 24, age: 27 },
              { p_id: "Bergkamp", goal: 95, caps: 98, age: 48 } ]
	 });

db.c13503217_teams.insert({
	team_id: "eng3",
	date_founded: new Date("Oct 04, 1912"),
     league: "Premier League",
	 points: 65,
	 name: "Chelsea",
     players: [ { p_id: "Costa", goal: 15, caps: 25, age: 28 },
              { p_id: "Ivanov", goal: 5, caps: 84, age: 28 },
			  { p_id: "Drogba", goal: 105, caps: 125, age: 35 } ]
	 });

db.c13503217_teams.insert({
	team_id: "spa1",
	date_founded: new Date("Oct 04, 1912"),
     league: "La Liga",
	 points: 80,
	 name: "Barcelona",
     players: [ { p_id: "Messi", goal: 195, caps: 189, age: 30 },
              { p_id: "Valdes", goal: 0, caps: 158, age: 27 },
			  { p_id: "Iniesta", goal: 72, caps: 25, age: 31},
			  { p_id: "Pique", goal: 9, caps: 201, age: 38 } ]
	 });

db.c13503217_teams.insert({
	team_id: "spa2",
	date_founded: new Date("Nov 04, 1914"),
     league: "La Liga",
	 points: 72,
	 name: "Real Madrid",
     players: [ { p_id: "Ronaldo", goal: 135, caps: 134, age: 28 },
				 { p_id: "Bale", goal: 75, caps: 45, age: 27 },
				 { p_id: "Marcelo", goal: 11, caps: 25, age: 31 },
                 { p_id: "Benzema", goal: 125, caps: 95, age: 22 } ]
	 });

db.c13503217_teams.insert({
	team_id: "spa3",
	date_founded: new Date("Oct 04, 1912"),
     league: "La liga",
	 points: 68,
	 name: "Valencia",
     players: [ { p_id: "Martinez", goal: 26, caps: 54, age: 21 },
              { p_id: "Aimar", goal: 45, caps: 105, age: 29 } ]
	 });

db.c13503217_teams.insert({
	team_id: "ita1",
	date_founded: new Date("Oct 04, 1922"),
     league: "Serie A",
	 points: 69,
	 name: "Roma",
     players: [ { p_id: "Totti", goal: 198, caps: 350, age: 35 },
              { p_id: "De Rossi", goal: 5, caps: 210, age: 30 },
			  { p_id: "Gervinho", goal: 43, caps: 57, age: 24 } ]
	 });

db.c13503217_teams.insert({
	team_id: "ita2",
	date_founded: new Date("Oct 04, 1899"),
     league: "Serie A",
	 points: 52,
	 name: "Juventus",
     players: [ { p_id: "Buffon", goal: 0, caps: 225, age: 37 },
              { p_id: "Pirlo", goal: 45, caps: 199, age: 35 },
			  { p_id: "Pogba", goal: 21, caps: 42, age: 20 } ]
	 });

db.c13503217_teams.insert({
	team_id: "ita3",
	date_founded: new Date("Oct 04, 1911"),
     league: "Serie A",
	 points: 62,
	 name: "AC Milan",
     players: [ { p_id: "Inzaghi", goal: 115, caps: 189, age: 35 },
              { p_id: "Abbiati", goal: 0, caps: 84, age: 24 },
			  { p_id: "Van Basten", goal: 123, caps: 104, age: 35 } ]
	 });

db.c13503217_teams.insert({
	team_id: "ita4",
	date_founded: new Date("Oct 04, 1902"),
     league: "Serie A",
	 points: 71,
	 name: "Inter Milan",
     players: [ { p_id: "Handanovic", goal: 0, caps: 51, age: 29 },
              { p_id: "Cambiasso", goal: 35, caps: 176, age: 35 },
			  { p_id: "Palacio", goal: 78, caps: 75, age: 31 } ]
	 });

//part 2

db.c13503217_teams.update (
    {"name" : "Manchester United"}, 
    {$push: {'players': {p_id:'Keane', goal: 33, caps: 326, age: 44}}}, 
    {upsert: true}
);

db.c13503217_teams.update (
    {"name" : "AC Milan"}, 
    {$push: {'players': {p_id:'Kaka', goal: 53, caps: 112, age: 32}}}, 
    {upsert: true}
);

//Part 3
db.c13503217_teams.find().sort({"date_founded": -1}).limit(1);

//part 4
db.c13503217_teams.find({ name: "Real Madrid" })
	.forEach(function (doc) {
		doc.players.forEach(function (player)
		{ player.goal += 3 })
		db.c13503217_teams.save(doc);
	});

//part6
db.c13503217_teams.find({name: "Barcelona"}).snapshot().forEach(
    function (elem) {
        db.c13503217_teams.update(
            {
                name: "Arsenal"
            },
            {
                $set: {
                    points: elem.points
                }
            }
        );
    }
);

//part7
db.c13503217_teams.find(
    { 
        players: {
            $elemMatch: {
                p_id: /es/, 
                age: { $gt: 30 }
            }
        }
    }, {'players.p_id': 1, 'players.age': 1});


//part8
db.c13503217_teams.aggregate([
    { 
        $group: {
            _id: '$league',
            totalPoints: { $sum: '$points'}
        }
    }
]);

//part9
db.c13503217_teams.aggregate([
    { $unwind: '$players' },
    { 
        $group: {
            _id: '$name',
            totalGoals: {$sum: '$players.goal' }
        }
    },
     { $sort: {totalGoals: -1}}
]);

//part10
db.c13503217_teams.aggregate([
    {$unwind : "$players"},
    {$group : {_id : "$players.p_id", total : {$sum: { $divide: ['$players.goal' , "$players.caps"]}}}},
    {$sort : { total : -1, posts: 1 } },
    {$out : "c13470112__avg_goals"}
]);

//part11
function old_vs_young(x) {
var Old = 0
var Young = 0
db.c13503217_teams.find()
  .forEach(function (doc) {
    doc.players.forEach(function (player) {
     if(x >= player.age) {
        Young = Young + player.goal}
     else {
        Old = Old + player.goal}
    })
    })
printjson("Goals over the age of " + x + " : "+Old);
printjson("Goals under the age of " + x + " : "+Young);
if(Old > Young) {
    return 1;}
else { 
    return 0;}
}
old_vs_young(40)