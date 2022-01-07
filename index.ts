import { readLine } from "https://raw.githubusercontent.com/deepakshrma/deno-by-example/master/examples/file_reader.ts";
import { exists } from "https://deno.land/std/fs/mod.ts";
import { Client } from "https://deno.land/x/mysql/mod.ts";
import * as path from "https://deno.land/std/path/mod.ts";
import ProgressBar from "https://deno.land/x/progress@v1.2.3/mod.ts";
// TODO:insert headers
// TODO; split csvs
class Tweet {
  id: number;
  text: string;
  date: string;
  userId: number;
  retweetCount: number;
  inReplyToTweetId: number | null;
  constructor(
    id: number,
    text: string,
    date: string,
    userId: number,
    retweetCount: number,
    inReplyToTweetId: number | null,
  ) {
    this.id = id;
    this.text = text;
    this.date = date.slice(0, 10);
    this.userId = userId;
    this.retweetCount = retweetCount;
    this.inReplyToTweetId = inReplyToTweetId;
  }
}

class User {
  id: number;
  userName: string;
  displayName: string;
  constructor(id: number, userName: string, displayName: string) {
    this.id = id;
    this.displayName = displayName;
    this.userName = userName;
  }
}

class DBManager {
  client: Client;
  version: number;
  static fixPath(location: string) {
    return location[0] == "/" ? location : path.join(Deno.cwd(), location);
  }
  async createTables() {
    //await this.client.execute(`DROP TABLE IF EXISTS tweets;`);
    await this.client.execute(`SET FOREIGN_KEY_CHECKS=0;`);
    await this.client.execute(
      `
      create or replace table users (
        user_id bigint not null,
        display_name varchar(100) NOT NULL,
        user_name varchar(100) NOT NULL,
        primary key(user_id)
      );`,
    );
    await this.client.execute(
      `
      create or replace table tweets (
        tweet_id bigint unsigned not null,
        text varchar(280) NOT NULL,
        date date NOT NULL,
        user_id bigint NOT NULL,
        retweet_count int unsigned NOT NULL,
        in_reply_to_tweet_id bigint unsigned,
        primary key(tweet_id)
      );`,
    );
    await this.client.execute(`
      create or replace table user_mentions(
        tweet_id bigint unsigned NOT NULL,
        user_id bigint NOT NULL,
        primary key(tweet_id, user_id)
      );
    `);
    console.log("Successfully reset tables.");
    //console.table(await this.client.execute("SHOW TABLES;"));
    //console.table(await this.client.execute(`show columns from tweets;`));
    //console.table(await this.client.execute(`show columns from users;`));
  }
  async insertTweet(tweet: Tweet) {
    return await this.client.execute(
      `INSERT IGNORE INTO tweets (
        tweet_id,
        text,
        date,
        user_id,
        retweet_count,
        in_reply_to_tweet_id
       )
       VALUES (
         ${tweet.id},
         ${JSON.stringify(tweet.text).replace("'", "'")},
         "${tweet.date}",
          ${tweet.userId},
          ${tweet.retweetCount},
          ${tweet.inReplyToTweetId}
        );`,
    );
  }
  async insertUser(user: User) {
    return await this.client.execute(`
          INSERT IGNORE INTO users (
            user_id,
            user_name,
            display_name
          )
          VALUES (
            ${user.id},
            '${user.userName}',
            QUOTE(${JSON.stringify(user.displayName)}) 
          )`);
    //console.log(response);
  }
  async getCount(tableName: string) {
    const response = await this.client.execute(
      `select count(*) from ${tableName}`,
    );
    return (response.rows! as Array<Record<string, number>>)[0]["count(*)"];
  }
  async insertUserMention(userId: number, tweetId: number) {
    return await this.client.execute(
      `
          INSERT IGNORE INTO user_mentions (user_id, tweet_id)
          VALUES (${userId}, '${tweetId}');
    `,
    );
  }
  async summary() {
    const tables = ["users", "tweets", "user_mentions"];
    for (const tableName of tables) {
      console.log(`'${tableName}' count: ${await this.getCount(tableName)}`);
      const response = await this.client.execute(`describe ${tableName}`);
      console.table(response.rows!);
    }
  }
  static async connect() {
    const client = await new Client().connect({
      hostname: "127.0.0.1",
      username: "root",
      db: "tweets",
      password: "ciao",
    });
    console.log("Connected to DB");
    return new DBManager(client);
  }
  private constructor(client: Client) {
    this.version = 0.99;
    this.client = client;
  }
  async importFolder(folderName: string) {
    for await (const dirData of Deno.readDir(folderName)) {
      const fileName = dirData["name"];
      if (fileName !== ".DS_Store") {
        await this.importFile(
          DBManager.fixPath(path.join(folderName, fileName)),
        );
      }
    }
  }
  async execute(sql: string) {
    const result = await this.client.execute(sql);
    const rows = result.rows!;
    for (const row of rows) {
      if ("text" in row) {
        row["text"] = (row["text"] as string).slice(0, 30) + "...";
      }
    }
    console.table(rows);
    console.log(`Count: ${result.rows!.length}`);
  }
  async exporttTable(folderName: string, tableName: string) {
  }
  async exportToCSV(folderName: string, tableName?: string) {
    const tables = tableName
      ? [tableName]
      : (await this.client.execute("show tables")).rows!.map((r) =>
        r["Tables_in_tweets"]
      );
    for (const tableName of tables) {
      const fileName = DBManager.fixPath(path.join(folderName, tableName));
      const sql = `
      SELECT * INTO OUTFILE '${fileName}.csv'
      FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
      LINES TERMINATED BY '\n'
      FROM ${tableName};`;
      await this.client.execute(sql);
      const fields = (await this.client.execute(`describe ${tableName}`))
        .rows!
        .map((r) => `"${r["Field"]}"`).join(",");
      const p = Deno.run({
        cmd: ["sed", "-i", `1i ${fields}`, fileName],
        stdout: "piped",
        stderr: "piped",
        stdin: "null",
      });
      await p.status();

      console.log(fields);
    }
  }
  async countLines(fileName: string) {
    const p = Deno.run({
      cmd: ["wc", "-l", fileName],
      stdout: "piped",
      stderr: "piped",
      stdin: "null",
    });
    await p.status();
    const response = (new TextDecoder().decode(await p.output()));
    console.log(response);
    console.log(response.split(" "));
    for (const element of response.split(" ")) {
      const num = Number(element);
      if (num > 0) {
        return num;
      }
    }
  }
  async importFile(fileName: string) {
    const lines = await this.countLines(fileName);
    const progress = new ProgressBar({
      total: lines,
      complete: "=",
      incomplete: "-",
      display: "Progress: :completed/:total :time [:bar] :percent",
    });
    console.log(`Importing file ${fileName}, this may take a while...`);
    console.log(`Number of lines in file ${lines}`);
    const reader = await readLine(fileName);
    let i = 0;
    for await (const value of reader) {
      let protoParsed: null | Record<string, unknown> = null;
      try {
        protoParsed = JSON.parse(value);
      } catch { /* */ }
      if (!(protoParsed === null)) {
        const tweetData = protoParsed as Record<string, unknown>;
        if (tweetData["lang"] === "it") {
          const userData = tweetData["user"] as Record<string, unknown>;
          const tweet = new Tweet(
            tweetData["id"] as number,
            tweetData["content"] as string,
            tweetData["date"] as string,
            userData["id"] as number,
            tweetData["retweetCount"] as number,
            tweetData["inReplyToTweetId"] as number,
          );
          const users = (tweetData["mentionedUsers"] != null
            ? [
              userData,
              ...(tweetData["mentionedUsers"] as Array<
                Record<string, unknown>
              >),
            ]
            : [userData]).map((x) =>
              new User(
                x["id"] as number,
                x["username"] as string,
                x["displayname"] as string,
              )
            );
          for (
            const [i, user] of users.entries()
          ) {
            if (i != 0) {
              await this.insertUserMention(user.id, tweet.id);
            }
            await this.insertUser(user);
          }
          await this.insertTweet(tweet);
        }
      }
      protoParsed = null;
      progress.render(i++);
    }
    console.log("Done importing file!!");
  }
}

async function main(action: string, parameter?: string) {
  const dbManager = await DBManager.connect();
  console.log(`Using version ${dbManager.version}`);
  switch (action) {
    case "import":
      if (parameter) {
        await dbManager.importFolder(parameter);
      }
      await dbManager.summary();
      break;
    case "reset":
      await dbManager.createTables();
      await dbManager.summary();
      break;
    case "export":
      if (parameter) {
        await dbManager.exportToCSV(parameter);
      }
      break;
    case "summary":
      await dbManager.summary();
      break;
    case "exec":
      if (parameter) {
        await dbManager.execute(parameter);
      }
      break;
    default:
      console.log(
        `
      Commands:

        import <folder-name>
        reset
        export <folder-name>
        summary
        exec <sql-command>
      `,
      );
      break;
  }
  Deno.exit();
}

await main(Deno.args[0], Deno.args.length > 1 ? Deno.args[1] : undefined);
