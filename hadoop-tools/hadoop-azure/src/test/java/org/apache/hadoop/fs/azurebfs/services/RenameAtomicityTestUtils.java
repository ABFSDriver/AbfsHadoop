package org.apache.hadoop.fs.azurebfs.services;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RenameAtomicityTestUtils {

  public static void addCreatePathMock(AbfsBlobClient client, Answer answer) {
    Mockito.doAnswer(clientHandlerAns -> {
          BlobRenameHandler renameHandler = Mockito.spy(
              (BlobRenameHandler) clientHandlerAns.callRealMethod());
          Mockito.doAnswer(getRenameAtomicityAns -> {
                RenameAtomicity renameAtomicity = Mockito.spy(
                    (RenameAtomicity) getRenameAtomicityAns.callRealMethod());
                Mockito.doAnswer(answer)
                    .when(renameAtomicity)
                    .createRenamePendingJson(Mockito.any(
                        Path.class), Mockito.any(byte[].class));
                return renameAtomicity;
              })
              .when(renameHandler)
              .getRenameAtomicity(Mockito.any(PathInformation.class));
          return renameHandler;
        })
        .when(client)
        .getBlobRenameHandler(Mockito.anyString(), Mockito.anyString(),
            Mockito.nullable(String.class), Mockito.anyBoolean(), Mockito.any(
                TracingContext.class));
  }

  public static void addReadPathMock(RenameAtomicity redoRenameAtomicity,
      Answer answer)
      throws AzureBlobFileSystemException {
    Mockito.doAnswer(answer)
        .when(redoRenameAtomicity)
        .readRenamePendingJson(Mockito.any(Path.class), Mockito.anyInt());
  }
}
