/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import com.alibaba.com.caucho.hessian.io.Hessian2Input;
import com.alibaba.com.caucho.hessian.io.Hessian2Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CompactStreamTaskSerde {

    public static final CompactStreamTaskSerde INSTANCE = new CompactStreamTaskSerde();


    public byte[] serialize(CompactStreamTask value) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Hessian2Output out = new Hessian2Output(os);
        try {
            out.writeObject(value);
            out.flush();
            return os.toByteArray();
        } finally {
            out.close();
            os.close();
        }
    }

    public CompactStreamTask deserialize(byte[] content) throws IOException {
        if (content.length == 0) {
            throw new IOException("The content is empty");
        }
        try (ByteArrayInputStream stream = new ByteArrayInputStream(content)) {
            Hessian2Input input = new Hessian2Input(stream);
            try {
                Object value = input.readObject();
                if (!(value instanceof CompactStreamTask task)) {
                    throw new IOException("Expected a compact stream task");
                }
                return task;
            } finally {
                input.close();
            }
        }
    }
}
