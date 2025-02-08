from logging import Logger, getLogger
from pathlib import Path
from typing import Any

import mlflow
from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from mlflow.entities import RunStatus


class CustomMlflowHook:
    @property
    def _logger(self) -> Logger:
        return getLogger(__name__)

    @hook_impl
    def after_context_created(
        self,
    ) -> None:
        mlflow.set_tracking_uri((Path().cwd() / "mlruns").as_uri())

    @hook_impl
    def before_pipeline_run(
        self, run_params: dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """Hook to be invoked before a pipeline runs.
        Args:
            run_params: The params needed for the given run.
                Should be identical to the data logged by Journal.
                # @fixme: this needs to be modelled explicitly as code, instead of comment
                Schema: {
                    "project_path": str,
                    "env": str,
                    "kedro_version": str,
                    "tags": Optional[List[str]],
                    "from_nodes": Optional[List[str]],
                    "to_nodes": Optional[List[str]],
                    "node_names": Optional[List[str]],
                    "from_inputs": Optional[List[str]],
                    "load_versions": Optional[List[str]],
                    "pipeline_name": str,
                    "extra_params": Optional[dict[str, Any]],
                }
            pipeline: The ``Pipeline`` that will be run.
            catalog: The ``DataCatalog`` to be used during the run.
        """

        mlflow.start_run(
            run_name=run_params["pipeline_name"],
            nested=True,
        )
        self._logger.info(
            f"Mlflow run '{mlflow.active_run().info.run_name}' has started"
        )

    @hook_impl
    def before_node_run(
        self, node: Node, catalog: DataCatalog, inputs: dict[str, Any], is_async: bool
    ) -> None:
        if not mlflow.active_run():
            self._logger.info(
                f"There is a bug: no mlflow run is active before node run"
            )
        params_inputs = {k[7:]: v for k, v in inputs.items() if k.startswith("params:")}
        mlflow.log_params(params_inputs)
        self._logger.info(
            f"node.name: '{node.name}', mlflow_run_id: '{mlflow.active_run().info.run_name}'"
        )

    @hook_impl
    def after_pipeline_run(
        self,
    ) -> None:
        mlflow.end_run()

    @hook_impl
    def on_pipeline_error(
        self,
    ):
        while mlflow.active_run():
            mlflow.end_run(RunStatus.to_string(RunStatus.FAILED))
