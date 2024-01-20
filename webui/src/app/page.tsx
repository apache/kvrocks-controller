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
