declare var require: any;
declare var process: any;
import {CQRSGraph} from "cubitt-graph-cqrs";
import {Command, CommandFactory} from "cubitt-commands";

var eb = vertx.eventBus();

// Initialize empty graph for now
var graph = new CQRSGraph();
let projectId = vertx.getOrCreateContext().config().id;

var JDBCClient = require("vertx-jdbc-js/jdbc_client");

let postgresUser = process.env.POSTGRES_USER || "postgres";
let postgresPass = process.env.POSTGRES_PASSWORD || "";
let postgresDb = process.env.POSTGRES_DB || postgresUser;
let postgresHost = process.env.POSTGRES_HOST || "localhost";
let postgresPort = process.env.POSTGRES_PORT || 5432;

var client = JDBCClient.createShared(vertx, {
  "url" : "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDb + "?user=" + postgresUser + "&password=" + postgresPass,
  "driver_class" : "org.postgresql.Driver",
  "max_pool_size" : 30
});

// Connect to database
client.getConnection(function (conn, conn_err) {
	if (conn_err != null) {
		console.log("Could not connect to database, shutting service down");
		vertx.close();
		return;
	}
	var connection = conn;
	// Load events for this project if they exist
	connection.query("SELECT event FROM \"" + projectId + "_events\"",function(res, res_err) {
		if (res_err) {
			console.log("Could not load events from eventstore " + res_err);
			vertx.close();
			return;
		}
		var results = res.results;
		Array.prototype.forEach.call(results, function(row) {
		  var event = row[0];
			graph.ApplyEvent(JSON.parse(event));
		});
		eb.consumer("projects.commands." + projectId, function (message) {
			var transaction = JSON.parse(message.body());

			var commands: Command[] = [];
			for (var com of transaction.commands) {
				try
				{
					// Syntax validation of commands
					var command: Command = CommandFactory.parse(com);
					commands.push(command);
				} catch(Error) {
					message.reply(JSON.stringify({ status: 400, data: null, error: Error.message + " for command " + JSON.stringify(com) }));
					return;
				}
			}
			if (commands.length == 0) {
				message.reply(JSON.stringify({ status: 400, data: null, error: "Commands should not be empty" }));
				return;
			}

			var closeConnection = function() {
				connection.close(function (done, done_err) {
					if (done_err) {
						return;
					}
				});
			}

			connection.setAutoCommit(false, function (res, res_err) {
				if (res_err != null) {
					console.log("Error disabling autocommit: " + res_err);
					message.reply(JSON.stringify({ status: 500, data: null, error: "Internal server error" }));
			    return;
			  }
				console.log("Disabled autocommit");
				// Rollback function
				var rollback = function(error, index) {
					connection.rollback(function(res,res_err) {
						if (res_err) {
							console.log("FUBAR?, rollback failed " + res_err);
							message.reply(JSON.stringify({ status: 500, data: null, error: "FUBAR" }));
							return;
						}
						console.log("Rollbacking");
						graph.Rollback();
						message.reply(JSON.stringify({ status: 400, data: null, error: error.message + " for command number " + index + " in transaction" }));
						return;
					});
				};
				var events = [];
				var applyCommand = function(index) {
					if (index == 0) {
						graph.BeginTransaction();
					}
					if (index === commands.length) {
						connection.commit(function(res, res_err) {
							if (res_err) {
								// Should be automatically rollbacked
								console.log("Failed to commit data " + res_err);
								graph.Rollback();
								return;
							}
							graph.CommitTransaction();
							message.reply(JSON.stringify({ status: 200, data: { version: graph.GetVersion() }, error: null }));
							for (let ev in events) {
								eb.publish("projects.events." + projectId, JSON.stringify(ev));
							}
							return;
						});
					} else {
						try
						{
							let comm = commands[index];
							let event = graph.ApplyCommand(comm);
							connection.updateWithParams(
								"INSERT INTO \"" + projectId + "_events\" VALUES(?,cast(? as json))",
								[graph.GetVersion(), event.toJson()],
								function (res, res_err) {
									if (res_err) {
										console.log("Inserting event failed " + res_err);
										rollback(new Error("Internal server error"), index);
										return;
									}
									events.push(event);
									applyCommand(index+1);
							});
						// This catch block is for the graph.ApplyCommand, not for the DB query
						// see (https://www.joyent.com/developers/node/design/errors#fn:1)
						} catch (error) {
							rollback(error, index);
							return;
						}
					}
				}
				applyCommand(0);
			});
		});
	});
});
