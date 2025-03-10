import { Client, GatewayIntentBits, Events } from "discord.js";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from 'dotenv';
dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DB_PATH = path.join(__dirname, "role_monitoring.db");
const TARGET_ROLES = ["PROVED UR LUV", "Prover", "PROOF OF ART", "PROOF OF DEV", "PROOF OF MUSIC", "PROOF OF WRITING", "PROOF OF VIDEO"]; // Роли, которые нужно отслеживать

// Каналы для мониторинга
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

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

async function initDatabase() {
  console.log("Инициализация базы данных...");
  
  const db = await open({
    filename: DB_PATH,
    driver: sqlite3.Database,
  });

  await db.run("PRAGMA journal_mode = WAL");
  await db.run("PRAGMA cache_size = -10000"); 
  await db.run("PRAGMA synchronous = NORMAL");
  await db.run("PRAGMA temp_store = MEMORY");
  await db.run("PRAGMA busy_timeout = 5000");

  await db.run(`
    CREATE TABLE IF NOT EXISTS channel_activity (
      user_id TEXT NOT NULL,
      username TEXT NOT NULL,
      roles TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      channel_name TEXT NOT NULL,
      message_count INTEGER DEFAULT 0,
      last_message TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, channel_id)
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(name)
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS snapshot_data (
      snapshot_id INTEGER NOT NULL,
      user_id TEXT NOT NULL,
      username TEXT NOT NULL,
      roles TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      channel_name TEXT NOT NULL,
      message_count INTEGER NOT NULL,
      PRIMARY KEY (snapshot_id, user_id, channel_id),
      FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
    )
  `);

  await db.run("CREATE INDEX IF NOT EXISTS idx_channel_activity_user ON channel_activity(user_id)");
  await db.run("CREATE INDEX IF NOT EXISTS idx_channel_activity_channel ON channel_activity(channel_id)");
  await db.run("CREATE INDEX IF NOT EXISTS idx_snapshot_data_snapshot ON snapshot_data(snapshot_id)");
  await db.run("CREATE INDEX IF NOT EXISTS idx_snapshot_data_user ON snapshot_data(user_id)");

  console.log("База данных инициализирована успешно");
  return db;
}


function hasTargetRole(member) {
  if (!member || !member.roles) return false;
  
  return member.roles.cache.some(role => 
    TARGET_ROLES.includes(role.name)
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

// Обработка нового сообщения
async function processMessage(db, message) {
  // Игнорируем сообщения от ботов
  if (message.author.bot) return;
  
  // Проверяем, находится ли канал в списке отслеживаемых
  if (!CHANNEL_IDS.includes(message.channel.id)) return;
  
  try {
    // Получаем информацию о пользователе
    const member = message.member || await message.guild.members.fetch(message.author.id).catch(() => null);
    
    // Проверяем, имеет ли пользователь целевую роль
    if (!member || !hasTargetRole(member)) return;
    
    const userId = message.author.id;
    const username = message.author.tag;
    const roles = getUserRoles(member);
    const channelId = message.channel.id;
    const channelName = message.channel.name;
    
    await db.run(`
      INSERT INTO channel_activity (user_id, username, roles, channel_id, channel_name, message_count, last_message)
      VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
      ON CONFLICT(user_id, channel_id) DO UPDATE SET
        username = ?,
        roles = ?,
        channel_name = ?,
        message_count = message_count + 1,
        last_message = CURRENT_TIMESTAMP
    `, [userId, username, roles, channelId, channelName, username, roles, channelName]);
    
    console.log(`[${new Date().toLocaleTimeString()}] Пользователь ${username} отправил сообщение в канале ${channelName}`);
    
  } catch (error) {
    console.error(`Ошибка при обработке сообщения: ${error.message}`);
  }
}


async function getRoleUserStats(db, limit = 20) {
  try {
    const users = await db.all(`
      SELECT 
        user_id, 
        username, 
        roles,
        SUM(message_count) as total_messages,
        MAX(last_message) as last_seen
      FROM channel_activity
      GROUP BY user_id
      ORDER BY total_messages DESC
      LIMIT ?
    `, [limit]);
    
    return users;
  } catch (error) {
    console.error(`Ошибка при получении статистики пользователей: ${error.message}`);
    return [];
  }
}

// Получение статистики по каналам для конкретного пользователя
async function getUserChannelStats(db, userId) {
  try {
    const channels = await db.all(`
      SELECT 
        channel_id,
        channel_name,
        message_count,
        last_message
      FROM channel_activity
      WHERE user_id = ?
      ORDER BY message_count DESC
    `, [userId]);
    
    return channels;
  } catch (error) {
    console.error(`Ошибка при получении статистики каналов для пользователя ${userId}: ${error.message}`);
    return [];
  }
}

async function createSnapshot(db) {
  try {
    const now = new Date();
    const name = now.toISOString().replace(/[:.]/g, '-');
    
    console.log(`Создание снапшота "${name}"...`);
    
    await db.run("BEGIN TRANSACTION");
    
    try {
      const result = await db.run(`
        INSERT INTO snapshots (name, created_at)
        VALUES (?, CURRENT_TIMESTAMP)
      `, [name]);
      
      const snapshotId = result.lastID;
      
      const data = await db.all(`
        SELECT 
          user_id,
          username,
          roles,
          channel_id,
          channel_name,
          message_count
        FROM channel_activity
        ORDER BY user_id, message_count DESC
      `);
      
      if (data.length === 0) {
        console.log("Нет данных для снапшота");
        await db.run("ROLLBACK");
        return null;
      }
      
    
      const chunkSize = 1000;
      for (let i = 0; i < data.length; i += chunkSize) {
        const chunk = data.slice(i, i + chunkSize);
        const placeholders = chunk.map(() => "(?, ?, ?, ?, ?, ?, ?)").join(", ");
        const values = [];
        
        for (const item of chunk) {
          values.push(
            snapshotId,
            item.user_id,
            item.username,
            item.roles,
            item.channel_id,
            item.channel_name,
            item.message_count
          );
        }
        
        await db.run(`
          INSERT INTO snapshot_data (
            snapshot_id, user_id, username, roles, channel_id, channel_name, message_count
          ) VALUES ${placeholders}
        `, values);
      }
      
      await db.run("COMMIT");
      
      console.log(`Снапшот "${name}" успешно создан (ID: ${snapshotId}, записей: ${data.length})`);
      
      return {
        id: snapshotId,
        name: name,
        count: data.length
      };
      
    } catch (error) {
      await db.run("ROLLBACK");
      console.error(`Ошибка при создании снапшота: ${error.message}`);
      return null;
    }
    
  } catch (error) {
    console.error(`Ошибка при создании снапшота: ${error.message}`);
    return null;
  }
}

async function cleanupOldSnapshots(db, keepCount = 100) {
  try {

    const snapshots = await db.all(`
      SELECT id
      FROM snapshots
      ORDER BY created_at DESC
    `);
    
    if (snapshots.length <= keepCount) {
      // Если снапшотов меньше или равно keepCount, ничего не делаем
      return;
    }
    
    // Получаем ID снапшотов, которые нужно удалить
    const snapshotsToDelete = snapshots.slice(keepCount).map(s => s.id);
    
    console.log(`Удаление ${snapshotsToDelete.length} старых снапшотов...`);
    
    await db.run("BEGIN TRANSACTION");
    
    try {
      // Удаляем снапшоты
      for (const id of snapshotsToDelete) {
        await db.run(`DELETE FROM snapshot_data WHERE snapshot_id = ?`, [id]);
        await db.run(`DELETE FROM snapshots WHERE id = ?`, [id]);
      }
      
      await db.run("COMMIT");
      
      console.log(`Удалено ${snapshotsToDelete.length} старых снапшотов`);
    } catch (error) {
      await db.run("ROLLBACK");
      console.error(`Ошибка при удалении старых снапшотов: ${error.message}`);
    }
  } catch (error) {
    console.error(`Ошибка при очистке старых снапшотов: ${error.message}`);
  }
}

// Вывод статистики в консоль
async function printStats(db) {
  console.log("\n=== Статистика пользователей с ролями ===");
  
  const users = await getRoleUserStats(db);
  
  if (users.length === 0) {
    console.log("Нет данных о пользователях с целевыми ролями");
  } else {
    console.log(`Топ-${users.length} пользователей по активности:`);
    
    for (const [index, user] of users.entries()) {
      console.log(`${index + 1}. ${user.username} [${user.roles}]`);
      console.log(`   Всего: ${user.total_messages} сообщений`);
      console.log(`   Последняя активность: ${new Date(user.last_seen).toLocaleString()}`);
      
      const channels = await getUserChannelStats(db, user.user_id);
      
      if (channels.length > 0) {
        console.log(`   Активность по каналам:`);
        for (const [chIndex, channel] of channels.entries()) {
          console.log(`     ${chIndex + 1}. ${channel.channel_name}: ${channel.message_count} сообщений`);
        }
      }
      
      console.log("");
    }
  }
}

function setupAutomaticSnapshots(db) {
  const SNAPSHOT_INTERVAL = 1000 * 60 * 60 * 4; // 4 час в миллисекундах
  
  console.log(`Настройка автоматического создания снапшотов каждые 4 часа`);
  
  setInterval(async () => {
    console.log(`Запланированное создание снапшота...`);
    await createSnapshot(db);
    await cleanupOldSnapshots(db);
  }, SNAPSHOT_INTERVAL)
}

async function main() {
  try {

    const db = await initDatabase();
    

    client.once(Events.ClientReady, async () => {
      console.log(`Бот ${client.user.tag} запущен и готов к работе!`);
      console.log(`Отслеживаемые роли: ${TARGET_ROLES.join(", ")}`);
      console.log(`Отслеживаемые каналы: ${CHANNEL_IDS.length}`);
      

      await printStats(db);
      

      setupAutomaticSnapshots(db);
    });
    
 
    client.on(Events.MessageCreate, async (message) => {
      await processMessage(db, message);
    });
    

    client.on(Events.Error, (error) => {
      console.error(`Ошибка клиента Discord: ${error.message}`);
    });
    
    process.on('SIGINT', async () => {
      console.log('Получен сигнал завершения, создаем финальный снапшот...');
      await createSnapshot(db);
      console.log('Завершение работы...');
      client.destroy();
      process.exit(0);
    });
    
 
    client.login(process.env.DISCORD_TOKEN);
    
  } catch (error) {
    console.error(`Критическая ошибка: ${error.message}`);
    process.exit(1);
  }
}

main();
