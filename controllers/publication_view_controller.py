class PublicationViewController:
    def __init__(self, db):
        self.publication_col = db["publication"]
        self.programs_col = db["programs"]
        self.publisher_col = db["publisher"]

    def get_publication_metadata(self, pub_id):
        """
        Retrieve metadata for a publication ID from:
        - publication
        - programs
        - publisher
        """

        # -------------------------
        # Publication (core)
        # -------------------------
        publication = self.publication_col.find_one(
            {"id": pub_id},
            {"_id": 0}
        )

        if not publication:
            return None  # ID truly does not exist

        # -------------------------
        # Programs (many)
        # -------------------------
        programs = list(
            self.programs_col.find(
                {"publication_id": pub_id},
                {"_id": 0}
            )
        )

        # -------------------------
        # Publisher (one)
        # -------------------------
        publisher = self.publisher_col.find_one(
            {"publication_id": pub_id},
            {"_id": 0}
        )

        # -------------------------
        # Combine response
        # -------------------------
        return {
            "publication": publication,
            "programs": programs,
            "publisher": publisher
        }
