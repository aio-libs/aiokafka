import abc


class AbstractMetricsReporter(metaclass=abc.ABCMeta):
    """
    An abstract class to allow things to listen as new metrics
    are created so they can be reported.
    """

    @abc.abstractmethod
    def init(self, metrics):
        """
        This is called when the reporter is first registered
        to initially register all existing metrics

        Arguments:
            metrics (list of KafkaMetric): All currently existing metrics
        """

    @abc.abstractmethod
    def metric_change(self, metric):
        """
        This is called whenever a metric is updated or added

        Arguments:
            metric (KafkaMetric)
        """

    @abc.abstractmethod
    def metric_removal(self, metric):
        """
        This is called whenever a metric is removed

        Arguments:
            metric (KafkaMetric)
        """

    @abc.abstractmethod
    def configure(self, configs):
        """
        Configure this class with the given key-value pairs

        Arguments:
            configs (dict of {str, ?})
        """

    @abc.abstractmethod
    def close(self):
        """Called when the metrics repository is closed."""
