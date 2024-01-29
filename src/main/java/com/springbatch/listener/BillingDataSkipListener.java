package com.springbatch.listener;

import com.springbatch.record.BillingData;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.IOException;
import java.nio.file.*;

public class BillingDataSkipListener implements SkipListener<BillingData, BillingData> {

    Path skippedItemsFile;

    public BillingDataSkipListener(String skippedItemsFile) {
        this.skippedItemsFile = Paths.get(skippedItemsFile);
    }
    @Override
    public void onSkipInRead(Throwable t) {
        if(t instanceof FlatFileParseException exception){
            String rawLine = exception.getInput();
            int lineNumber = exception.getLineNumber();
            String skippedLine = lineNumber+"|"+rawLine+System.lineSeparator();
            try{
                Files.writeString(skippedItemsFile, skippedLine, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new RuntimeException("Unable to write skipped line: "+skippedLine);
            }
        }
    }
}
