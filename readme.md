POST => /api/add - adding user to one of 3 dbs(Mongo, Cassandra, PostgreSQL)
=>{username:..., email:...}

GET => /users/"id" - getting user by id
=>{success: bool, message: String, user: json}

GET => /api/users - getting all users from 3dbs
=>{success: bool, message: String, allUsers: json}

PUT => /users/"id" - updating user by id => {username:..., email:...}

=>{success: bool, message: String, user: json(postgres returns empty array)}

DELETE => /users/"id" - deleting user by id
=>{success: bool, message: String}

GET => /api/search - full text search => ?query=...
=>{success: bool, message: String, foundUsers: json}
