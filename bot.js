import { Client, GatewayIntentBits } from "discord.js";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from 'dotenv';
dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CHANNEL_IDS = [
  "1339397379866103830",
  "1085300495414984722",
  "1342308270437695609",
  "1347812089087131719",
  "1339707793095131240",
  "1248226016887963689",
  "1248228076928761938",
  "1248708006322114612",
  "1248708270126927893",
  "1248227012867522631",
  "1248709078075576430",
  "1248396964719100047",
  "1248151962348687492",
  "1248125430590865499",
  "1337556213470072952",
  "1085300495574372423",
  "1339134460519518259"
];
const DB_PATH = path.join(__dirname, "discord_stats.db");
const TARGET_ROLES = ["PROVED UR LUV", "Prover"];

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

async function initDatabase() {
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database,
  });

  await db.run("PRAGMA journal_mode = WAL");
  await db.run("PRAGMA cache_size = -100000"); 
  await db.run("PRAGMA synchronous = NORMAL");
  await db.run("PRAGMA temp_store = MEMORY");
  await db.run("PRAGMA mmap_size = 30000000000"); 
  await db.run("PRAGMA page_size = 32768");
  await db.run("PRAGMA busy_timeout = 5000");
  await db.run("PRAGMA threads = 4");

  await db.run(`
    CREATE TABLE IF NOT EXISTS metadata (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS user_stats (
      user_id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      roles TEXT DEFAULT '',
      message_count INTEGER DEFAULT 0,
      word_count INTEGER DEFAULT 0,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS channel_last_message (
      channel_id TEXT PRIMARY KEY,
      last_message_id TEXT NOT NULL,
      last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS word_stats (
      word TEXT PRIMARY KEY,
      count INTEGER DEFAULT 0
    )
  `);

  await db.run("CREATE INDEX IF NOT EXISTS idx_word_stats_count ON word_stats(count)");
  await db.run("CREATE INDEX IF NOT EXISTS idx_user_stats_message_count ON user_stats(message_count)");

  return db;
}

async function getLastProcessedMessageId(db, channelId) {
  const row = await db.get(
    "SELECT last_message_id FROM channel_last_message WHERE channel_id = ?",
    [channelId]
  );
  return row ? row.last_message_id : null;
}

async function saveLastProcessedMessageId(db, channelId, messageId) {
  await db.run(
    `INSERT INTO channel_last_message (channel_id, last_message_id, last_updated) 
     VALUES (?, ?, CURRENT_TIMESTAMP)
     ON CONFLICT(channel_id) DO UPDATE SET
       last_message_id = ?,
       last_updated = CURRENT_TIMESTAMP`,
    [channelId, messageId, messageId]
  );
}

function getUserRoles(member) {
  if (!member || !member.roles) return "";
  
  const roles = member.roles.cache
    .filter(role => role.name !== "@everyone")
    .map(role => role.name)
    .join(", ");
  
  return roles;
}

async function processChannelMessages(db, channel, maxMessages = 1000000) {
  const channelId = channel.id;
  const channelName = channel.name || channelId;
  const guildName = channel.guild ? channel.guild.name : "Неизвестный сервер";
  
  console.log(`\n=== Начинаю сбор сообщений из канала ${channelName} (${guildName}) ===`);
  
  const startTime = Date.now();
  let messagesProcessed = 0;
  let lastMessageId = await getLastProcessedMessageId(db, channelId);
  
  if (lastMessageId) {
    console.log(`Последнее обработанное сообщение: ${lastMessageId}`);
  } else {
    console.log(`Первый запуск для этого канала, начинаю с самых новых сообщений`);
  }
  
  let newestMessageId = null;
  try {
    const newestMessages = await channel.messages.fetch({ limit: 1 });
    if (newestMessages.size > 0) {
      newestMessageId = newestMessages.first().id;
      console.log(`Самое новое сообщение в канале: ${newestMessageId}`);
    }
  } catch (error) {
    console.error(`Ошибка при получении последнего сообщения: ${error.message}`);
    return 0;
  }
  
  if (lastMessageId && lastMessageId === newestMessageId) {
    console.log(`Нет новых сообщений в канале ${channelName} с момента последнего запуска`);
    return 0;
  }
  
  const BATCH_SIZE = 1000000;
  let userStats = {};
  let wordCounts = {};
  let batchCounter = 0;
  
  async function commitBatch() {
    const userCount = Object.keys(userStats).length;
    const wordCount = Object.keys(wordCounts).length;
    
    if (userCount === 0 && wordCount === 0) return;
    
    console.log(`Коммит пакета данных: ${userCount} пользователей, ${wordCount} слов`);
    
    await db.run("BEGIN TRANSACTION");
    
    try {
      if (userCount > 0) {
        const stmt = await db.prepare(`
          INSERT INTO user_stats (user_id, username, roles, message_count, word_count, last_updated)
          VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          ON CONFLICT(user_id) DO UPDATE SET
            username = ?,
            roles = ?,
            message_count = message_count + ?,
            word_count = word_count + ?,
            last_updated = CURRENT_TIMESTAMP
        `);
        
        for (const userId in userStats) {
          const stats = userStats[userId];
          await stmt.run(
            userId,
            stats.username,
            stats.roles,
            stats.messageCount,
            stats.wordCount,
            stats.username,
            stats.roles,
            stats.messageCount,
            stats.wordCount
          );
        }
        
        await stmt.finalize();
      }
      
      if (wordCount > 0) {
        const stmt = await db.prepare(`
          INSERT INTO word_stats (word, count)
          VALUES (?, ?)
          ON CONFLICT(word) DO UPDATE SET
            count = count + ?
        `);
        
        for (const word in wordCounts) {
          const count = wordCounts[word];
          await stmt.run(word, count, count);
        }
        
        await stmt.finalize();
      }
      
      await db.run("COMMIT");
      
      userStats = {};
      wordCounts = {};
      batchCounter = 0;
      
    } catch (error) {
      await db.run("ROLLBACK");
      console.error(`Ошибка при коммите данных: ${error.message}`);
    }
  }
  
  function processMessage(message, roles) {
    const userId = message.author.id;
    const username = message.author.tag;
    
    if (!userStats[userId]) {
      userStats[userId] = {
        username,
        roles,
        messageCount: 0,
        wordCount: 0
      };
    }
    
    userStats[userId].messageCount++;
    
    const content = message.content || "";
    
    // Учитываем сообщение даже если это вложение или ссылка
    if (content.trim() === "" && (message.attachments.size > 0 || message.embeds.length > 0)) {
      // Это сообщение с вложением или встроенным контентом, но без текста
      // Увеличиваем только счетчик сообщений, но не слов
    } else {
      // Удаляем ссылки, упоминания и эмодзи перед подсчетом слов
      const cleanContent = content
        .replace(/https?:\/\/\S+/g, "") // Ссылки
        .replace(/<@!?\d+>/g, "") // Упоминания пользователей
        .replace(/<#\d+>/g, "") // Упоминания каналов
        .replace(/<@&\d+>/g, "") // Упоминания ролей
      
      const words = cleanContent.toLowerCase().match(/[a-z]{2,}/g) || [];
      
      if (words.length > 0) {
        userStats[userId].wordCount += words.length;
        
        for (const word of words) {
          wordCounts[word] = (wordCounts[word] || 0) + 1;
        }
      }
    }
    
    batchCounter++;
    
    if (batchCounter >= BATCH_SIZE) {
      return true;
    }
    
    return false;
  }
  
  if (!lastMessageId) {
    let fetchOptions = { limit: 100 };
    
    while (messagesProcessed < maxMessages) {
      try {
        const fetchedMessages = await channel.messages.fetch(fetchOptions);
        
        if (fetchedMessages.size === 0) {
          console.log(`Достигнут конец истории канала`);
          break;
        }
        
        const userRolesCache = {};
        
        if (channel.guild) {
          const uniqueUserIds = [...new Set(Array.from(fetchedMessages.values()).map(msg => msg.author.id))];
          
          for (const userId of uniqueUserIds) {
            try {
              const member = await channel.guild.members.fetch(userId);
              userRolesCache[userId] = getUserRoles(member);
            } catch (error) {
              userRolesCache[userId] = "";
            }
          }
        }
        
        for (const [id, message] of fetchedMessages) {
          const roles = userRolesCache[message.author.id] || "";
          
          const shouldCommit = processMessage(message, roles);
          
          if (shouldCommit) {
            await commitBatch();
          }
          
          if (!newestMessageId || BigInt(message.id) > BigInt(newestMessageId)) {
            newestMessageId = message.id;
          }
        }
        
        messagesProcessed += fetchedMessages.size;
        
        fetchOptions.before = fetchedMessages.last().id;
        
        if (messagesProcessed % 1000 === 0) {
          const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1);
          const messagesPerSecond = (messagesProcessed / elapsedSeconds).toFixed(1);
          console.log(`Обработано: ${messagesProcessed} сообщений (${messagesPerSecond} сообщений/сек)`);
        }
        
      } catch (error) {
        console.error(`Ошибка при получении сообщений: ${error.message}`);
        
        if (error.httpStatus === 429) {
          const retryAfter = error.retryAfter || 5;
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        } else {
          await new Promise(resolve => setTimeout(resolve, 3000));
        }
      }
    }
  } else if (lastMessageId !== newestMessageId) {
    console.log(`Получаем новые сообщения после ${lastMessageId}`);
    
    let fetchOptions = { limit: 100, after: lastMessageId };
    
    while (messagesProcessed < maxMessages) {
      try {
        const fetchedMessages = await channel.messages.fetch(fetchOptions);
        
        if (fetchedMessages.size === 0) {
          console.log(`Нет новых сообщений после ${lastMessageId}`);
          break;
        }
        
        const sortedMessages = Array.from(fetchedMessages.values())
          .sort((a, b) => a.createdTimestamp - b.createdTimestamp);
        
        const userRolesCache = {};
        
        if (channel.guild) {
          const uniqueUserIds = [...new Set(sortedMessages.map(msg => msg.author.id))];
          
          for (const userId of uniqueUserIds) {
            try {
              const member = await channel.guild.members.fetch(userId);
              userRolesCache[userId] = getUserRoles(member);
            } catch (error) {
              userRolesCache[userId] = "";
            }
          }
        }
        
        for (const message of sortedMessages) {
          const roles = userRolesCache[message.author.id] || "";
          
          const shouldCommit = processMessage(message, roles);
          
          if (shouldCommit) {
            await commitBatch();
          }
          
          if (!newestMessageId || BigInt(message.id) > BigInt(newestMessageId)) {
            newestMessageId = message.id;
          }
        }
        
        messagesProcessed += sortedMessages.length;
        
        const latestMessage = sortedMessages.reduce((latest, msg) => 
          !latest || BigInt(msg.id) > BigInt(latest.id) ? msg : latest, null);
        
        if (latestMessage) {
          fetchOptions.after = latestMessage.id;
        }
        
        if (messagesProcessed % 1000 === 0) {
          const elapsedSeconds = ((Date.now() - startTime) / 1000).toFixed(1);
          const messagesPerSecond = (messagesProcessed / elapsedSeconds).toFixed(1);
          console.log(`Обработано: ${messagesProcessed} сообщений (${messagesPerSecond} сообщений/сек)`);
        }
        
      } catch (error) {
        console.error(`Ошибка при получении сообщений: ${error.message}`);
        
        if (error.httpStatus === 429) {
          const retryAfter = error.retryAfter || 5;
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
        } else {
          await new Promise(resolve => setTimeout(resolve, 3000));
        }
      }
    }
  }
  
  if (batchCounter > 0) {
    await commitBatch();
  }
  
  if (newestMessageId) {
    await saveLastProcessedMessageId(db, channelId, newestMessageId);
    console.log(`Сохранен ID последнего сообщения для канала ${channelName}: ${newestMessageId}`);
  }
  
  console.log(`=== Сбор сообщений из канала ${channelName} завершен. Всего обработано: ${messagesProcessed} сообщений ===`);
  
  return messagesProcessed;
}

async function collectAndAnalyzeMessages(db) {
  try {
    const startTime = Date.now();
    const MAX_MESSAGES_PER_CHANNEL = 1000000;
    let totalMessagesProcessed = 0;
    
    console.log(`\n=== Используем фиксированный список из ${CHANNEL_IDS.length} каналов ===`);
    
    for (const channelId of CHANNEL_IDS) {
      try {
        if (global.gc) {
          global.gc();
        }
        
        const channel = await client.channels.fetch(channelId);
        if (!channel) {
          console.log(`Канал ${channelId} не найден, пропускаем`);
          continue;
        }
        
        const messagesProcessed = await processChannelMessages(db, channel, MAX_MESSAGES_PER_CHANNEL);
        totalMessagesProcessed += messagesProcessed;
        
        if (global.gc) {
          global.gc();
        }
        
        await new Promise(resolve => setTimeout(resolve, 2000));
        
      } catch (error) {
        console.error(`Ошибка при обработке канала ${channelId}: ${error.message}`);
      }
    }
    
    console.log(`\n=== Общее время выполнения сбора сообщений: ${((Date.now() - startTime) / 1000).toFixed(1)} секунд ===`);
    console.log(`=== Всего обработано ${totalMessagesProcessed} сообщений из ${CHANNEL_IDS.length} каналов ===\n`);
    
    console.log("\nАнализ популярных слов...");
    const topWords = await getTopWords(db, 20);
    console.log("ТОП-20 самых популярных английских слов:");
    topWords.forEach((row, index) => {
      console.log(`${index + 1}. ${row.word} — ${row.count} раз`);
    });
    
    console.log("\nАнализ активности пользователей...");
    const topUsers = await getTopUsers(db, 20);
    console.log("ТОП-20 самых активных пользователей:");
    topUsers.forEach((row, index) => {
      console.log(`${index + 1}. ${row.username} ${row.roles ? `[${row.roles}]` : ""}`);
      console.log(`   Всего: ${row.message_count} сообщений, ${row.word_count} слов`);
    });
    
    console.log(`\nАнализ активности пользователей с ролью "${TARGET_ROLES.join(", ")}"...`);
    const roleUsers = await getUsersWithRole(db, TARGET_ROLES, 20);
    console.log(`Статистика пользователей с ролью "${TARGET_ROLES.join(", ")}":`);
    roleUsers.forEach((row, index) => {
      console.log(`${index + 1}. ${row.username}`);
      console.log(`   Всего: ${row.message_count} сообщений, ${row.word_count} слов`);
    });
    
  } catch (error) {
    console.error(`Ошибка при сборе и анализе сообщений: ${error.message}`);
  }
}

async function getTopWords(db, limit = 20) {
  const commonWords = [
    "a", "an", "and", "are", "as", "at", "for", "from", "but", "by", 
    "in", "into", "is", "it", "of", "on", "that", "the", "to", "was",
    "with", "he", "she", "his", "her", "they", "them", "their",
    "which", "who", "whom", "this", "was", "will", "would"
  ].map(word => `'${word}'`).join(",");

  return await db.all(`
    SELECT word, count
    FROM word_stats
    WHERE word NOT IN (${commonWords})
    ORDER BY count DESC
    LIMIT ?
  `, [limit]);
}

async function getTopUsers(db, limit = 20) {
  return await db.all(`
    SELECT 
      username, 
      roles,
      message_count, 
      word_count
    FROM user_stats
    ORDER BY message_count DESC
    LIMIT ?
  `, [limit]);
}

async function getUsersWithRole(db, roleNames, limit = 20) {
  try {
    const conditions = roleNames
      .map((role) => {
        return `(roles LIKE '${role},%' OR roles LIKE '%, ${role},%' OR roles LIKE '%, ${role}' OR roles = '${role}')`;
      })
      .join(" OR ");

    return await db.all(`
      SELECT 
        user_id,
        username, 
        roles,
        message_count, 
        word_count
      FROM user_stats
      WHERE ${conditions}
      ORDER BY message_count DESC
      LIMIT ?
    `, [limit]);
  } catch (error) {
    console.error(`Ошибка при получении пользователей с ролями ${roleNames.join(", ")}: ${error.message}`);
    return [];
  }
}

async function createRoleUserSnapshot(db, roleNames, snapshotName, topActiveLimit = 1000) {
  try {
    console.log(`\n=== Создание снепшота пользователей ===`);
    
    await db.run(`
      CREATE TABLE IF NOT EXISTS role_user_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_name TEXT NOT NULL,
        user_id TEXT NOT NULL,
        username TEXT NOT NULL,
        roles TEXT DEFAULT '',
        message_count INTEGER NOT NULL,
        word_count INTEGER NOT NULL,
        has_target_role BOOLEAN NOT NULL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(snapshot_name, user_id)
      )
    `);

    await db.run(`
      CREATE INDEX IF NOT EXISTS idx_role_user_snapshots_name 
      ON role_user_snapshots(snapshot_name)
    `);

    const conditions = roleNames
      .map((role) => {
        return `(roles LIKE '${role},%' OR roles LIKE '%, ${role},%' OR roles LIKE '%, ${role}' OR roles = '${role}')`;
      })
      .join(" OR ");

    console.log(`Получение пользователей с ролями ${roleNames.join(", ")}...`);
    const roleUsers = await db.all(`
      SELECT user_id, username, roles, message_count, word_count
      FROM user_stats
      WHERE ${conditions}
      ORDER BY message_count DESC
    `);
    console.log(`Найдено ${roleUsers.length} пользователей с целевыми ролями`);

    console.log(`Получение топ-${topActiveLimit} активных пользователей...`);
    const topActiveUsers = await db.all(`
      SELECT user_id, username, roles, message_count, word_count
      FROM user_stats
      ORDER BY message_count DESC
      LIMIT ?
    `, [topActiveLimit]);
    console.log(`Получено ${topActiveUsers.length} активных пользователей`);

    const userMap = new Map();

    for (const user of roleUsers) {
      userMap.set(user.user_id, { ...user, has_target_role: 1 });
    }

    for (const user of topActiveUsers) {
      if (!userMap.has(user.user_id)) {
        userMap.set(user.user_id, { ...user, has_target_role: 0 });
      }
    }

    const allUsers = Array.from(userMap.values());

    if (allUsers.length === 0) {
      console.log(`⚠️ Не найдено пользователей для снепшота`);
      return;
    }

    // Создаем имя снепшота с датой и временем в читаемом формате
    const now = new Date();
    const formattedDate = now.toISOString().replace('T', ' ').substring(0, 19);
    const finalSnapshotName = snapshotName || formattedDate;

    console.log(`Создание снепшота "${finalSnapshotName}"...`);

    await db.run("BEGIN TRANSACTION");

    try {
      await db.run(`DELETE FROM role_user_snapshots WHERE snapshot_name = ?`, [finalSnapshotName]);

      const chunkSize = 1000;

      for (let i = 0; i < allUsers.length; i += chunkSize) {
        const chunk = allUsers.slice(i, i + chunkSize);
        const placeholders = chunk.map(() => "(?, ?, ?, ?, ?, ?, ?)").join(", ");
        const values = [];

        for (const user of chunk) {
          values.push(
            finalSnapshotName,
            user.user_id,
            user.username,
            user.roles || "",
            user.message_count,
            user.word_count,
            user.has_target_role
          );
        }

        await db.run(`
          INSERT INTO role_user_snapshots (
            snapshot_name, user_id, username, roles, message_count, word_count, has_target_role
          ) VALUES ${placeholders}
        `, values);

        console.log(`Обработано ${i + chunk.length} из ${allUsers.length} пользователей`);
      }

      await db.run("COMMIT");

      const roleUserCount = roleUsers.length;
      console.log(`✅ Снепшот "${finalSnapshotName}" создан успешно:`);
      console.log(`   - ${roleUserCount} пользователей с целевыми ролями`);
      console.log(`   - ${allUsers.length - roleUserCount} дополнительных активных пользователей`);
      console.log(`   - Всего ${allUsers.length} пользователей в снепшоте`);

      return finalSnapshotName;
    } catch (error) {
      await db.run("ROLLBACK");
      console.error(`❌ Ошибка при создании снепшота: ${error.message}`);
      throw error;
    }
  } catch (error) {
    console.error(`❌ Ошибка при создании снепшота: ${error.message}`);
    return null;
  }
}

async function listSnapshots(db, limit = 30) {
  try {
    console.log(`\n=== Получение списка последних ${limit} снепшотов ===`);

    const snapshots = await db.all(`
      SELECT 
        snapshot_name, 
        COUNT(*) as total_users,
        SUM(CASE WHEN has_target_role = 1 THEN 1 ELSE 0 END) as role_users,
        MIN(created_at) as created_at
      FROM role_user_snapshots
      GROUP BY snapshot_name
      ORDER BY created_at DESC
      LIMIT ?
    `, [limit]);

    console.log("\nПоследние снепшоты:");

    if (snapshots.length === 0) {
      console.log("Снепшоты не найдены");
      return [];
    }

    snapshots.forEach((snapshot, index) => {
      const date = new Date(snapshot.created_at).toLocaleString();
      console.log(`${index + 1}. ${snapshot.snapshot_name}`);
      console.log(`   Всего: ${snapshot.total_users} пользователей (${snapshot.role_users} с целевыми ролями)`);
      console.log(`   Создан: ${date}`);
    });

    return snapshots;
  } catch (error) {
    console.error(`❌ Ошибка при получении списка снепшотов: ${error.message}`);
    return [];
  }
}

async function cleanupOldSnapshots(db, keepCount = 90) {
  try {
    console.log(`Проверка необходимости очистки старых снепшотов...`);

    const allSnapshots = await db.all(`
      SELECT DISTINCT snapshot_name, MIN(created_at) as created_at
      FROM role_user_snapshots
      GROUP BY snapshot_name
      ORDER BY created_at DESC
    `);

    if (allSnapshots.length <= keepCount) {
      console.log(`Всего ${allSnapshots.length} снепшотов, очистка не требуется`);
      return;
    }

    const snapshotsToDelete = allSnapshots.slice(keepCount).map(s => s.snapshot_name);

    console.log(`Удаление ${snapshotsToDelete.length} старых снепшотов...`);

    await db.run("BEGIN TRANSACTION");

    try {
      const chunkSize = 10;

      for (let i = 0; i < snapshotsToDelete.length; i += chunkSize) {
        const chunk = snapshotsToDelete.slice(i, i + chunkSize);
        const placeholders = chunk.map(() => "?").join(",");

        await db.run(`DELETE FROM role_user_snapshots WHERE snapshot_name IN (${placeholders})`, chunk);

        console.log(`Удалено ${i + chunk.length} из ${snapshotsToDelete.length} снепшотов`);
      }

      await db.run("COMMIT");
      console.log(`✅ Удалено ${snapshotsToDelete.length} старых снепшотов, оставлено ${keepCount} последних`);
    } catch (error) {
      await db.run("ROLLBACK");
      console.error(`❌ Ошибка при удалении старых снепшотов: ${error.message}`);
      throw error;
    }
  } catch (error) {
    console.error(`❌ Ошибка при очистке старых снепшотов: ${error.message}`);
  }
}

client.once("ready", async () => {
  console.log(`Бот ${client.user.tag} запущен и готов к работе!`);

  try {
    const db = await open({
      filename: DB_PATH,
      driver: sqlite3.Database,
    });

    await db.run("PRAGMA journal_mode = WAL");
    await db.run("PRAGMA cache_size = -100000"); 
    await db.run("PRAGMA synchronous = NORMAL");
    await db.run("PRAGMA temp_store = MEMORY");
    await db.run("PRAGMA optimize");

    await initDatabase();
    console.log("База данных инициализирована успешно");

    // Получаем аргументы командной строки
    const args = process.argv.slice(2);
    const command = args[0];

    if (command === "list-snapshots") {
      // Просто выводим список снепшотов
      await listSnapshots(db);
    } else if (command === "create-snapshot") {
      // Создаем снепшот с текущей датой в качестве имени
      const today = new Date().toISOString().split("T")[0];
      await createRoleUserSnapshot(db, TARGET_ROLES, today);
      await listSnapshots(db);
    } else if (command === "cleanup-snapshots") {
      // Очищаем старые снепшоты, оставляя только последние 90 (или указанное количество)
      const keepCount = args.length >= 2 ? parseInt(args[1]) : 90;
      await cleanupOldSnapshots(db, keepCount);
      await listSnapshots(db);
    } else {
      // Стандартный режим - анализ сообщений и создание снепшота с текущей датой
      await collectAndAnalyzeMessages(db);

      // Создаем снепшот для пользователей с целевыми ролями и топ активных
      // Используем только дату в качестве имени снепшота
      const today = new Date().toISOString().split("T")[0];

      await createRoleUserSnapshot(db, TARGET_ROLES, today);

      // Выводим список доступных снепшотов
      await listSnapshots(db);

      // Очищаем старые снепшоты, оставляя только последние 90
      await cleanupOldSnapshots(db, 90);
    }

    // Закрываем базу данных
    await db.close();

    console.log("Работа завершена, выход...");
    client.destroy();
    process.exit(0);
  } catch (error) {
    console.error(`Критическая ошибка: ${error.message}`);
    client.destroy();
    process.exit(1);
  }
});

client.on("error", (error) => {
  console.error(`Ошибка клиента Discord: ${error.message}`);
});

client.login(process.env.DISCORD_TOKEN);
