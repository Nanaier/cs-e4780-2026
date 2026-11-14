package fi.aalto.cse4780.trading;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;

public class PathOrderSplitAssigner implements FileSplitAssigner {

    private final PriorityQueue<FileSourceSplit> pendingSplits =
            new PriorityQueue<>(Comparator.comparing((FileSourceSplit split) -> split.path().toString()));

    public PathOrderSplitAssigner(Collection<FileSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public Optional<FileSourceSplit> getNext(String hostname) {
        return Optional.ofNullable(pendingSplits.poll());
    }

    @Override
    public void addSplits(Collection<FileSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public Collection<FileSourceSplit> remainingSplits() {
        return new ArrayList<>(pendingSplits);
    }
}
