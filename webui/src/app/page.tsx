/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

import { Button } from "@mui/material";
import Image from "next/image";

export default function Home() {
    return (
        <main className="flex flex-col items-center justify-center min-h-screen space-y-4">
            <h1 className="text-4xl font-bold">Kvrocks Controler UI</h1>
            <p className="text-xl">Work in progress...</p>
            <Button size="large" variant="outlined" sx={{ textTransform: 'none' }} href="https://github.com/apache/kvrocks-controller/issues/135">
                Click here to submit your suggestions
            </Button>
        </main>
    );
}
