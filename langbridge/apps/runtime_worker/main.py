import argparse
import asyncio
import logging
import os
from typing import Sequence

from dependency_injector import providers
from redis.exceptions import RedisError

from langbridge.packages.common.langbridge_common.db.session_context import reset_session, set_session
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import (
    MessageEnvelope,
)
from langbridge.packages.common.langbridge_common.monitoring import start_metrics_server
from .broker.customer_runtime import CustomerRuntimeBroker
from .handlers import WorkerMessageDispatcher
from .ioc import create_container, DependencyResolver


async def run_worker(poll_interval: float = 2.0) -> None:
    logger = logging.getLogger("langbridge.worker")
    worker_concurrency = _read_positive_int_env("WORKER_CONCURRENCY", default=10, logger=logger)
    batch_size = _read_positive_int_env(
        "WORKER_BATCH_SIZE",
        default=worker_concurrency,
        logger=logger,
    )
    processing_semaphore = asyncio.Semaphore(worker_concurrency)
    consume_timeout_ms = max(1000, int(poll_interval * 1000))
    logger.info(
        "Worker starting. Poll interval: %s seconds. Concurrency: %s. Batch size: %s",
        poll_interval,
        worker_concurrency,
        batch_size,
    )

    run_once = os.environ.get("WORKER_RUN_ONCE", "false").lower() in {"1", "true", "yes"}
    broker_mode = os.environ.get("WORKER_BROKER", "redis").lower()
    execution_mode = os.environ.get("WORKER_EXECUTION_MODE", "hosted").strip().lower()
    use_database_session = execution_mode not in {"customer_runtime", "customer-runtime", "edge"}

    container = create_container()
    container.wire(packages=["langbridge.apps.runtime_worker"])
    dependency_resolver = DependencyResolver(container)
    worker_dispatcher = WorkerMessageDispatcher(dependency_resolver=dependency_resolver)

    if broker_mode in {"none", "noop", "disabled"}:
        broker = _NoopBroker()
        logger.info("Worker broker disabled; running in noop mode")
    elif execution_mode in {"customer_runtime", "customer-runtime", "edge"}:
        broker = CustomerRuntimeBroker()
        logger.info("Worker running in customer-runtime mode via edge gateway transport")
    else:
        broker = container.message_broker()
    message_broker_provider: providers.Provider | None = getattr(container, "message_broker", None)
    if hasattr(message_broker_provider, "override"):
        message_broker_provider.override(providers.Object(broker))

    in_flight_tasks: set[asyncio.Task[None]] = set()
    idle_logged = False
    try:
        while True:
            if len(in_flight_tasks) >= worker_concurrency:
                await asyncio.wait(in_flight_tasks, return_when=asyncio.FIRST_COMPLETED)
                continue

            available_slots = worker_concurrency - len(in_flight_tasks)
            try:
                messages = await broker.consume(
                    timeout_ms=consume_timeout_ms,
                    count=min(batch_size, available_slots),
                )
            except RedisError as exc:
                logger.error("Redis error: %s", exc)
                if run_once:
                    return
                await asyncio.sleep(poll_interval)
                continue
            except Exception as exc:
                logger.error("Worker consume error: %s", exc)
                if run_once:
                    return
                await asyncio.sleep(poll_interval)
                continue

            if not messages:
                if not idle_logged:
                    logger.info("Worker idle: awaiting jobs")
                    idle_logged = True
                if run_once:
                    logger.info("Worker run-once enabled; exiting.")
                    return
            else:
                idle_logged = False
                for message in messages:
                    task = asyncio.create_task(
                        _process_message(
                            message=message,
                            container=container,
                            worker_dispatcher=worker_dispatcher,
                            broker=broker,
                            logger=logger,
                            processing_semaphore=processing_semaphore,
                            use_database_session=use_database_session,
                        ) 
                    )
                    in_flight_tasks.add(task)
                    _track_in_flight_task(task=task, in_flight_tasks=in_flight_tasks, logger=logger)
                if run_once:
                    logger.info("Worker run-once enabled; exiting.")
                    return
    finally:
        if in_flight_tasks:
            await asyncio.gather(*in_flight_tasks, return_exceptions=True)
        await broker.close()


async def _process_message(
    *,
    message,
    container,
    worker_dispatcher: WorkerMessageDispatcher,
    broker,
    logger: logging.Logger,
    processing_semaphore: asyncio.Semaphore,
    use_database_session: bool,
) -> None:
    async with processing_semaphore:
        envelope = message.envelope
        logger.info(
            "Received message %s (%s)",
            envelope.id,
            envelope.message_type,
        )
        try:
            if use_database_session:
                session_factory = container.async_session_factory()
                session = session_factory()
                token = set_session(session)
                try:
                    logger.debug("UnitOfWork: starting async DB session")
                    new_messages: Sequence[MessageEnvelope] | None = await worker_dispatcher.handle_message(
                        envelope
                    )
                    logger.debug("UnitOfWork: committing session")
                    await session.commit()
                except BaseException as exc:
                    logger.error("UnitOfWork: rolling back due to exception: %s", exc)
                    await session.rollback()
                    raise
                finally:
                    reset_session(token)
                    await session.close()
                    logger.debug("UnitOfWork: session closed")
            else:
                new_messages = await worker_dispatcher.handle_message(envelope)
            if new_messages:
                for new_message in new_messages:
                    await broker.publish(new_message)
        except Exception as exc:
            logger.error("Handler error: %s", exc)
            await broker.nack(message, error=str(exc))
            return
        await broker.ack(message)


def _track_in_flight_task(
    *,
    task: asyncio.Task[None],
    in_flight_tasks: set[asyncio.Task[None]],
    logger: logging.Logger,
) -> None:
    def _on_done(done_task: asyncio.Task[None]) -> None:
        in_flight_tasks.discard(done_task)
        try:
            done_task.result()
        except asyncio.CancelledError:
            logger.warning("Worker message task was cancelled.")
        except Exception:
            logger.exception("Unhandled worker message task error.")

    task.add_done_callback(_on_done)


def _read_positive_int_env(
    env_name: str,
    *,
    default: int,
    logger: logging.Logger,
) -> int:
    raw_value = os.environ.get(env_name)
    if raw_value is None:
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        logger.warning(
            "Invalid %s value '%s'; falling back to %s.",
            env_name,
            raw_value,
            default,
        )
        return default
    if parsed < 1:
        logger.warning(
            "Invalid %s value '%s'; falling back to %s.",
            env_name,
            raw_value,
            default,
        )
        return default
    return parsed


def _run_once() -> None:
    logging.basicConfig(
        level=os.environ.get("WORKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    poll_interval = float(os.environ.get("WORKER_POLL_INTERVAL", "2.0"))
    metrics_port = int(os.environ.get("WORKER_METRICS_PORT", "9101"))
    start_metrics_server(metrics_port)
    asyncio.run(run_worker(poll_interval=poll_interval))


def _run_with_reload() -> None:
    try:
        from watchfiles import run_process
    except ImportError as exc:
        raise RuntimeError("`watchfiles` is required to run the worker in reload mode.") from exc

    run_process(
        # paths=["langbridge"],
        target=_run_once,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LangBridge worker.")
    parser.add_argument("--reload", action="store_true", help="Restart on source changes.")
    args = parser.parse_args()
    reload_env = os.environ.get("WORKER_RELOAD", "false").lower() in {"1", "true", "yes"}

    if args.reload or reload_env:
        _run_with_reload()
    else:
        _run_once()


class _NoopBroker:
    async def publish(self, message: MessageEnvelope, stream: str | None = None) -> str:
        return "noop"

    async def consume(self, *, timeout_ms: int, count: int):
        return []

    async def ack(self, message) -> None:
        return

    async def nack(self, message, *, error: str | None = None) -> None:
        return

    async def close(self) -> None:
        return


if __name__ == "__main__":
    main()
