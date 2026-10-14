import type { Metadata } from "next";
import { GeistSans } from "geist/font/sans";
import "./globals.css";

export const metadata: Metadata = {
  title: "ConfigStream · Research evidence",
  icons: { icon: "/favicon.svg" },
  description:
    "Verified infrastructure history, scoped states, native occurrences and uncertain measurements.",
};
export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${GeistSans.variable} antialiased`}>{children}</body>
    </html>
  );
}
