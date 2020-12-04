package org.apache.flink.connector.jdbc;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Types;
import java.time.Duration;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INPUT_TABLE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.INSERT_TEMPLATE;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TEST_DATA;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.TestEntry;
import static org.apache.flink.connector.jdbc.JdbcTestFixture.getCreateQuery;

/**
 * This test deals with sql driver class loading issues, write it alone so it won't be
 * interfered by other tests.
 */
public class JdbcDriverClassConcurrentLoadingTest {
	@ClassRule
	public static MySQLContainer<?> mySQLContainer = new MySQLContainer("mysql:8.0.22") {
		/**
		 * Override {@link JdbcDatabaseContainer#waitUntilContainerStarted()} to not touch driver class.
		 */
		@Override
		protected void waitUntilContainerStarted() {
			getWaitStrategy().waitUntilReady(this);
			Duration timeout = Duration.ofSeconds(getStartupTimeoutSeconds());
			long endTime = System.nanoTime() + timeout.toNanos();
			while (true) {
				try {
					Container.ExecResult execResult = execInContainer(
						"mysql",
						"-u" + mySQLContainer.getUsername(),
						"-p" + mySQLContainer.getPassword(),
						"-e",
						mySQLContainer.getTestQueryString(),
						mySQLContainer.getDatabaseName()
					);
					if (execResult.getExitCode() == 0) {
						return;
					} else if (System.nanoTime() >= endTime) {
						String msg = String.format(
							"MySQL container has not started fully within %s seconds",
							timeout.getSeconds());
						throw new RuntimeException(msg);
					}
					Thread.sleep(2000);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					return;
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			}
		}
	};

	@ClassRule
	public static PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:12.5");

	private static void execContainerCommand(GenericContainer<?> container, String... command) throws Exception {
		Container.ExecResult execResult = container.execInContainer(command);
		if (execResult.getExitCode() != 0) {
			String msg = String.format(
				"Fail to execute container command, exit code: %d, stdout: %s, stderr: %s",
				execResult.getExitCode(), execResult.getStdout(), execResult.getStderr()
			);
			throw new RuntimeException(msg);
		}
	}

	@Test
	public void testDriverClassConcurrentLoading() throws Exception {
		execContainerCommand(mySQLContainer,
			"mysql",
			"-u" + mySQLContainer.getUsername(),
			"-p" + mySQLContainer.getPassword(),
			"-e",
			getCreateQuery(INPUT_TABLE),
			mySQLContainer.getDatabaseName()
		);
		execContainerCommand(postgreSQLContainer,
			"psql",
			"-U",
			postgreSQLContainer.getUsername(),
			"-d",
			postgreSQLContainer.getDatabaseName(),
			"-c",
			getCreateQuery(INPUT_TABLE)
		);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.setParallelism(1);

		DataStreamSource<TestEntry> source = env.fromElements(TEST_DATA);

		// Disable chaining so that operators run in different, thus concurrent, tasks.
		source.disableChaining();

		JdbcStatementBuilder<TestEntry> statementBuilder = (ps, t) -> {
			ps.setInt(1, t.id);
			ps.setString(2, t.title);
			ps.setString(3, t.author);
			ps.setObject(4, t.price, Types.DOUBLE);
			ps.setInt(5, t.qty);
		};

		DataStreamSink<TestEntry> mysqlSink = source.addSink(JdbcSink.sink(
			String.format(INSERT_TEMPLATE, INPUT_TABLE),
			statementBuilder,
			new JdbcConnectionOptionsBuilder()
				.withUrl(mySQLContainer.getJdbcUrl())
				.withDriverName("com.mysql.cj.jdbc.Driver")
				.withUsername(mySQLContainer.getUsername())
				.withPassword(mySQLContainer.getPassword())
				.build()
		));
		mysqlSink.name("MySQL Jdbc Sink");

		DataStreamSink<TestEntry> postgresSink = source.addSink(JdbcSink.sink(
			String.format(INSERT_TEMPLATE, INPUT_TABLE),
			statementBuilder,
			new JdbcConnectionOptionsBuilder()
				.withUrl(postgreSQLContainer.getJdbcUrl())
				.withDriverName("org.postgresql.Driver")
				.withUsername(postgreSQLContainer.getUsername())
				.withPassword(postgreSQLContainer.getPassword())
				.build()
		));
		postgresSink.name("Postgres Jdbc Sink");

		env.execute();
	}
}
